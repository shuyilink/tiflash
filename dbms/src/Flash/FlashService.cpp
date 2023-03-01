// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/CPUAffinityManager.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadMetricUtil.h>
#include <Common/TiFlashMetrics.h>
#include <Common/VariantOp.h>
#include <Common/getNumberOfCPUCores.h>
#include <Common/setThreadName.h>
#include <Flash/BatchCoprocessorHandler.h>
#include <Flash/EstablishCall.h>
#include <Flash/FlashService.h>
#include <Flash/Management/ManualCompact.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/Utils.h>
#include <Flash/ServiceUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Server/IServer.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/TMTContext.h>
#include <grpcpp/server_builder.h>
#include <TiDB/Schema/SchemaSyncer.h>

#include <ext/scope_guard.h>
#include "boost/algorithm/string/join.hpp"
#include "common/logger_useful.h"

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

#define CATCH_FLASHSERVICE_EXCEPTION                                                                                                        \
    catch (Exception & e)                                                                                                                   \
    {                                                                                                                                       \
        LOG_ERROR(log, "DB Exception: {}", e.message());                                                                                    \
        return std::make_tuple(std::make_shared<Context>(*context), grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message())); \
    }                                                                                                                                       \
    catch (const std::exception & e)                                                                                                        \
    {                                                                                                                                       \
        LOG_ERROR(log, "std exception: {}", e.what());                                                                                      \
        return std::make_tuple(std::make_shared<Context>(*context), grpc::Status(grpc::StatusCode::INTERNAL, e.what()));                    \
    }                                                                                                                                       \
    catch (...)                                                                                                                             \
    {                                                                                                                                       \
        LOG_ERROR(log, "other exception");                                                                                                  \
        return std::make_tuple(std::make_shared<Context>(*context), grpc::Status(grpc::StatusCode::INTERNAL, "other exception"));           \
    }

constexpr char tls_err_msg[] = "common name check is failed";

FlashService::FlashService() = default;

void FlashService::init(const TiFlashSecurityConfig & security_config_, Context & context_)
{
    security_config = &security_config_;
    context = &context_;
    log = &Poco::Logger::get("FlashService");
    manual_compact_manager = std::make_unique<Management::ManualCompactManager>(
        context->getGlobalContext(),
        context->getGlobalContext().getSettingsRef());

    auto settings = context->getSettingsRef();
    enable_local_tunnel = settings.enable_local_tunnel;
    enable_async_grpc_client = settings.enable_async_grpc_client;
    const size_t default_size = getNumberOfLogicalCPUCores();

    auto cop_pool_size = static_cast<size_t>(settings.cop_pool_size);
    cop_pool_size = cop_pool_size ? cop_pool_size : default_size;
    LOG_INFO(log, "Use a thread pool with {} threads to handle cop requests.", cop_pool_size);
    cop_pool = std::make_unique<ThreadPool>(cop_pool_size, [] { setThreadName("cop-pool"); });

    auto batch_cop_pool_size = static_cast<size_t>(settings.batch_cop_pool_size);
    batch_cop_pool_size = batch_cop_pool_size ? batch_cop_pool_size : default_size;
    LOG_INFO(log, "Use a thread pool with {} threads to handle batch cop requests.", batch_cop_pool_size);
    batch_cop_pool = std::make_unique<ThreadPool>(batch_cop_pool_size, [] { setThreadName("batch-cop-pool"); });
}

FlashService::~FlashService() = default;

// Use executeInThreadPool to submit job to thread pool which return grpc::Status.
grpc::Status executeInThreadPool(ThreadPool & pool, std::function<grpc::Status()> job)
{
    std::packaged_task<grpc::Status()> task(job);
    std::future<grpc::Status> future = task.get_future();
    pool.schedule([&task] { task(); });
    return future.get();
}

String getClientMetaVarWithDefault(const grpc::ServerContext * grpc_context, const String & name, const String & default_val)
{
    if (auto it = grpc_context->client_metadata().find(name); it != grpc_context->client_metadata().end())
        return String(it->second.data(), it->second.size());

    return default_val;
}

grpc::Status FlashService::Coprocessor(
    grpc::ServerContext * grpc_context,
    const coprocessor::Request * request,
    coprocessor::Response * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_DEBUG(log, "Handling coprocessor request: {}", request->DebugString());

    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    bool is_remote_read = getClientMetaVarWithDefault(grpc_context, "is_remote_read", "") == "true";
    GET_METRIC(tiflash_coprocessor_request_count, type_cop).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Increment();
    if (is_remote_read)
    {
        GET_METRIC(tiflash_coprocessor_request_count, type_remote_read).Increment();
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_remote_read).Increment();
    }
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_cop).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes, type_cop).Increment(response->ByteSizeLong());
        if (is_remote_read)
            GET_METRIC(tiflash_coprocessor_handling_request_count, type_remote_read).Decrement();
    });

    context->setMockStorage(mock_storage);

    grpc::Status ret = executeInThreadPool(*cop_pool, [&] {
        auto [db_context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        if (is_remote_read)
            GET_METRIC(tiflash_coprocessor_handling_request_count, type_remote_read_executing).Increment();
        SCOPE_EXIT({
            if (is_remote_read)
                GET_METRIC(tiflash_coprocessor_handling_request_count, type_remote_read_executing).Decrement();
        });
        CoprocessorContext cop_context(*db_context, request->context(), *grpc_context);
        CoprocessorHandler cop_handler(cop_context, request, response);
        return cop_handler.execute();
    });

    LOG_DEBUG(log, "Handle coprocessor request done: {}, {}", ret.error_code(), ret.error_message());
    return ret;
}

grpc::Status FlashService::BatchCoprocessor(grpc::ServerContext * grpc_context, const coprocessor::BatchRequest * request, grpc::ServerWriter<coprocessor::BatchResponse> * writer)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_DEBUG(log, "Handling coprocessor request: {}", request->DebugString());

    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    GET_METRIC(tiflash_coprocessor_request_count, type_batch).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_batch).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_batch).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_batch).Observe(watch.elapsedSeconds());
        // TODO: update the value of metric tiflash_coprocessor_response_bytes.
    });

    grpc::Status ret = executeInThreadPool(*batch_cop_pool, [&] {
        auto [db_context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        CoprocessorContext cop_context(*db_context, request->context(), *grpc_context);
        BatchCoprocessorHandler cop_handler(cop_context, request, writer);
        return cop_handler.execute();
    });

    LOG_DEBUG(log, "Handle coprocessor request done: {}, {}", ret.error_code(), ret.error_message());
    return ret;
}

grpc::Status FlashService::DispatchMPPTask(
    grpc::ServerContext * grpc_context,
    const mpp::DispatchTaskRequest * request,
    mpp::DispatchTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_DEBUG(log, "Handling mpp dispatch request: {}", request->DebugString());
    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;
    GET_METRIC(tiflash_coprocessor_request_count, type_dispatch_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_dispatch_mpp_task).Increment();
    GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Increment();
    GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Increment();
    if (!tryToResetMaxThreadsMetrics())
    {
        GET_METRIC(tiflash_thread_count, type_max_threads_of_dispatch_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_dispatch_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Value()));
        GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Value(), GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Value()));
    }

    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Decrement();
        GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Decrement();
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_dispatch_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_dispatch_mpp_task).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes, type_dispatch_mpp_task).Increment(response->ByteSizeLong());
    });

    auto [db_context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return status;
    }
    db_context->setMockStorage(mock_storage);
    db_context->setMockMPPServerInfo(mpp_test_info);

    MPPHandler mpp_handler(*request);
    return mpp_handler.execute(db_context, response);
}

grpc::Status FlashService::IsAlive(grpc::ServerContext * grpc_context [[maybe_unused]],
                                   const mpp::IsAliveRequest * request [[maybe_unused]],
                                   mpp::IsAliveResponse * response [[maybe_unused]])
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    auto & tmt_context = context->getTMTContext();
    response->set_available(tmt_context.checkRunning());
    return grpc::Status::OK;
}

grpc::Status AsyncFlashService::establishMPPConnectionAsync(grpc::ServerContext * grpc_context,
                                                            const mpp::EstablishMPPConnectionRequest * request,
                                                            EstablishCallData * call_data)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // Establish a pipe for data transferring. The pipes have registered by the task in advance.
    // We need to find it out and bind the grpc stream with it.
    LOG_DEBUG(log, "Handling establish mpp connection request: {}", request->DebugString());

    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    GET_METRIC(tiflash_coprocessor_request_count, type_mpp_establish_conn).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Increment();

    call_data->startEstablishConnection();
    call_data->tryConnectTunnel();
    return grpc::Status::OK;
}

grpc::Status FlashService::EstablishMPPConnection(grpc::ServerContext * grpc_context, const mpp::EstablishMPPConnectionRequest * request, grpc::ServerWriter<mpp::MPPDataPacket> * sync_writer)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // Establish a pipe for data transferring. The pipes have registered by the task in advance.
    // We need to find it out and bind the grpc stream with it.
    LOG_DEBUG(log, "Handling establish mpp connection request: {}", request->DebugString());

    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;
    GET_METRIC(tiflash_coprocessor_request_count, type_mpp_establish_conn).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Increment();
    GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Increment();
    GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Increment();
    if (!tryToResetMaxThreadsMetrics())
    {
        GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Value()));
        GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Value(), GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Value()));
    }
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Decrement();
        GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Decrement();
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_mpp_establish_conn).Observe(watch.elapsedSeconds());
        // TODO: update the value of metric tiflash_coprocessor_response_bytes.
    });

    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    std::chrono::seconds timeout(10);
    auto [tunnel, err_msg] = task_manager->findTunnelWithTimeout(request, timeout);
    if (tunnel == nullptr)
    {
        if (!sync_writer->Write(getPacketWithError(err_msg)))
        {
            LOG_DEBUG(log, "Write error message failed for unknown reason.");
            return grpc::Status(grpc::StatusCode::UNKNOWN, "Write error message failed for unknown reason.");
        }
    }
    else
    {
        Stopwatch stopwatch;
        SyncPacketWriter writer(sync_writer);
        tunnel->connect(&writer);
        tunnel->waitForFinish();
        LOG_INFO(tunnel->getLogger(), "connection for {} cost {} ms.", tunnel->id(), stopwatch.elapsedMilliseconds());
    }
    return grpc::Status::OK;
}

grpc::Status FlashService::CancelMPPTask(
    grpc::ServerContext * grpc_context,
    const mpp::CancelTaskRequest * request,
    mpp::CancelTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // CancelMPPTask cancels the query of the task.
    LOG_DEBUG(log, "cancel mpp task request: {}", request->DebugString());

    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;
    GET_METRIC(tiflash_coprocessor_request_count, type_cancel_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_cancel_mpp_task).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_cancel_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_cancel_mpp_task).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes, type_cancel_mpp_task).Increment(response->ByteSizeLong());
    });

    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    task_manager->abortMPPQuery(request->meta().start_ts(), "Receive cancel request from TiDB", AbortType::ONCANCELLATION);
    return grpc::Status::OK;
}

std::tuple<ContextPtr, grpc::Status> FlashService::createDBContextForTest() const
{
    try
    {
        /// Create DB context.
        auto tmp_context = std::make_shared<Context>(*context);
        tmp_context->setGlobalContext(*context);

        String query_id;
        tmp_context->setCurrentQueryId(query_id);
        ClientInfo & client_info = tmp_context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;

        String max_threads;
        tmp_context->setSetting("enable_async_server", is_async ? "true" : "false");
        tmp_context->setSetting("enable_local_tunnel", enable_local_tunnel ? "true" : "false");
        tmp_context->setSetting("enable_async_grpc_client", enable_async_grpc_client ? "true" : "false");
        return std::make_tuple(tmp_context, grpc::Status::OK);
    }
    CATCH_FLASHSERVICE_EXCEPTION
}

::grpc::Status FlashService::cancelMPPTaskForTest(const ::mpp::CancelTaskRequest * request, ::mpp::CancelTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // CancelMPPTask cancels the query of the task.
    LOG_DEBUG(log, "cancel mpp task request: {}", request->DebugString());
    auto [context, status] = createDBContextForTest();
    if (!status.ok())
    {
        auto err = std::make_unique<mpp::Error>();
        err->set_msg("error status");
        response->set_allocated_error(err.release());
        return status;
    }
    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    task_manager->abortMPPQuery(request->meta().start_ts(), "Receive cancel request from GTest", AbortType::ONCANCELLATION);
    return grpc::Status::OK;
}

grpc::Status FlashService::checkGrpcContext(const grpc::ServerContext * grpc_context) const
{
    // For coprocessor/mpp test, we don't care about security config.
    if likely (!context->isMPPTest() && !context->isCopTest())
    {
        if (!security_config->checkGrpcContext(grpc_context))
        {
            return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
        }
    }
    std::string peer = grpc_context->peer();
    Int64 pos = peer.find(':');
    if (pos == -1)
    {
        return grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid peer address: " + peer);
    }
    return grpc::Status::OK;
}

std::tuple<ContextPtr, grpc::Status> FlashService::createDBContext(const grpc::ServerContext * grpc_context) const
{
    try
    {
        /// Create DB context.
        auto tmp_context = std::make_shared<Context>(*context);
        tmp_context->setGlobalContext(*context);

        /// Set a bunch of client information.
        std::string user = getClientMetaVarWithDefault(grpc_context, "user", "default");
        std::string password = getClientMetaVarWithDefault(grpc_context, "password", "");
        std::string quota_key = getClientMetaVarWithDefault(grpc_context, "quota_key", "");
        std::string peer = grpc_context->peer();
        Int64 pos = peer.find(':');
        std::string client_ip = peer.substr(pos + 1);
        Poco::Net::SocketAddress client_address(client_ip);

        // For MPP or Cop test, we don't care about security config.
        if (likely(!context->isTest()))
            tmp_context->setUser(user, password, client_address, quota_key);

        String query_id = getClientMetaVarWithDefault(grpc_context, "query_id", "");
        tmp_context->setCurrentQueryId(query_id);

        ClientInfo & client_info = tmp_context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;

        /// Set DAG parameters.
        std::string dag_records_per_chunk_str = getClientMetaVarWithDefault(grpc_context, "dag_records_per_chunk", "");
        if (!dag_records_per_chunk_str.empty())
        {
            tmp_context->setSetting("dag_records_per_chunk", dag_records_per_chunk_str);
        }

        String max_threads = getClientMetaVarWithDefault(grpc_context, "tidb_max_tiflash_threads", "");
        if (!max_threads.empty())
        {
            tmp_context->setSetting("max_threads", max_threads);
            LOG_INFO(log, "set context setting max_threads to {}", max_threads);
        }

        tmp_context->setSetting("enable_async_server", is_async ? "true" : "false");
        tmp_context->setSetting("enable_local_tunnel", enable_local_tunnel ? "true" : "false");
        tmp_context->setSetting("enable_async_grpc_client", enable_async_grpc_client ? "true" : "false");
        return std::make_tuple(tmp_context, grpc::Status::OK);
    }
    CATCH_FLASHSERVICE_EXCEPTION
}

grpc::Status FlashService::Compact(grpc::ServerContext * grpc_context, const kvrpcpb::CompactRequest * request, kvrpcpb::CompactResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    return manual_compact_manager->handleRequest(request, response);
}

void FlashService::setMockStorage(MockStorage & mock_storage_)
{
    mock_storage = mock_storage_;
}

void FlashService::setMockMPPServerInfo(MockMPPServerInfo & mpp_test_info_)
{
    mpp_test_info = mpp_test_info_;
}

MvccQueryInfo::RegionsQueryInfo makeRegionsQueryInfo(TableID base_table_id,
                                                     const ::coprocessor::TableRegions & table_regions,
                                                     const Context & context [[maybe_unused]])
{
    if (table_regions.physical_table_id() != base_table_id)
        throw DB::Exception(fmt::format(
            "physical_table_id[{}] not match the base_table_id[{}]",
            table_regions.physical_table_id(), base_table_id));

    MvccQueryInfo::RegionsQueryInfo regions_info;

    regions_info.reserve(table_regions.regions_size());
    for (const auto & region_info: table_regions.regions())
    {
        const auto & epoch = region_info.region_epoch();
        RegionQueryInfo region_query_info(region_info.region_id(), epoch.version(), epoch.conf_ver(), base_table_id);

        region_query_info.required_handle_ranges.reserve(region_info.ranges_size());
        for (const auto & kv_range : region_info.ranges())
        {
            TiKVKey start_key = RecordKVFormat::encodeAsTiKVKey(kv_range.start());
            TiKVKey end_key = RecordKVFormat::encodeAsTiKVKey(kv_range.end());
            RegionRangeKeys region_range(std::move(start_key), std::move(end_key));
            region_query_info.required_handle_ranges.emplace_back(region_range.rawKeys());
        }

        regions_info.emplace_back(std::move(region_query_info));
    }
    return regions_info;
}

std::string concatenate(const char * prefix, Int64 db_id) { 
    return prefix + std::to_string(db_id);
}

grpc::Status FlashService::FillMaterializedView(grpc::ServerContext * grpc_context,
                                                const mpp::FillMaterializedViewRequest * request,
                                                mpp::FillMaterializedViewResponse * response)
{
    auto [db_context, status] = createDBContext(grpc_context);
    if (unlikely(!status.ok()))
        return status;

    std::string mv_db_name = concatenate("db_", request->mv_db_id());
    std::string base_db_name = concatenate("db_", request->base_db_id());
    std::string mv_table_name = concatenate("t_", request->mv_table_id());
    std::string base_table_name = concatenate("t_", request->base_table_id());

    try
    {
        if (db_context->hasPartitionMvCompleted(mv_db_name, mv_table_name))
            return status;
        db_context->setPartitionMvCompleted(mv_db_name, mv_table_name);

        auto & global_context = db_context->getGlobalContext();
        auto base_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(
                    db_context->getTable(base_db_name, base_table_name));
        auto mv_storage = [&](const ContextPtr & context) {
            StorageDeltaMergePtr inner_mv_storage = nullptr;
            try
            {
               inner_mv_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(
                    context->getTable(mv_db_name, mv_table_name));
            }
            catch(const Exception & e)
            {
                context->getTMTContext().getSchemaSyncer()->syncSchemas(global_context);
                inner_mv_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(
                    context->getTable(mv_db_name, mv_table_name));
            }
            inner_mv_storage->clearData();
            return inner_mv_storage;
        }(db_context);

        auto mv_create_query = std::dynamic_pointer_cast<ASTCreateQuery>(
                db_context->getCreateTableQuery(mv_db_name, mv_table_name));
        auto insert_query = std::make_shared<ASTInsertQuery>();
        insert_query->database = mv_db_name;
        insert_query->table = mv_table_name;
        insert_query->select = mv_create_query->select->clone();
        auto regions_query_info = makeRegionsQueryInfo(
                request->base_table_id(),
                request->table_regions(),
                global_context);
        InterpreterInsertQuery(insert_query, global_context)
        .setRegionsQueryInfo(std::move(regions_query_info))
        .execute();

        mv_storage->flushCache(global_context);
        LOG_DEBUG(log,
                  "Success to FillMaterializedView: base[{}.{}], mv[{}.{}], table_regions[{}]",
                  base_db_name, base_table_name, mv_db_name, mv_table_name,
                  request->table_regions().DebugString());
        return status;
    }
    catch (const Exception & e)
    {
        db_context->setPartitionMvImcompleted(mv_db_name, mv_table_name);
        response->set_error(fmt::format(
            "Fail to FillMaterializedView: base[{}.{}], mv[{}.{}], message[{}]",
            base_db_name, base_table_name, mv_db_name, mv_table_name, e.message()));
        LOG_WARNING(log, response->error());
        return grpc::Status(grpc::StatusCode::INTERNAL, response->error());
    }
    catch (const std::exception & e)
    {
        db_context->setPartitionMvImcompleted(mv_db_name, mv_table_name);
        response->set_error(fmt::format(
            "Fail to FillMaterializedView: base[{}.{}], mv[{}.{}], what[{}]",
            base_db_name, base_table_name, mv_db_name, mv_table_name, e.what()));
        LOG_WARNING(log, response->error());
        return grpc::Status(grpc::StatusCode::INTERNAL, response->error());
    }
}
} // namespace DB
