#pragma once

#include <Common/Allocator.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V3/Remote/RemoteDataInfo.h>
#include <Storages/Page/V3/PageEntry.h>


namespace DB
{
class RemotePageReader : private Allocator<false>
{
public:
    explicit RemotePageReader(const String & remote_directory_)
        :remote_directory(remote_directory_)
    {}

    Page read(const PS::V3::RemoteDataLocation & location, const PS::V3::PageEntryV3 & page_entry);

    Page read(const PS::V3::RemoteDataLocation & location, const PS::V3::PageEntryV3 & page_entry, std::vector<size_t> fields);

private:
    String remote_directory;
};

using RemotePageReaderPtr = std::shared_ptr<RemotePageReader>;
}
