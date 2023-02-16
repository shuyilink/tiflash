#include "RemotePageReader.h"
#include <IO/ReadBufferFromFile.h>


namespace DB
{
Page RemotePageReader::read(const PS::V3::RemoteDataLocation & location, const PS::V3::PageEntryV3 & page_entry)
{
    auto buf = std::make_shared<ReadBufferFromFile>(remote_directory + *location.data_file_id);
    buf->seek(location.offset_in_file);
    auto buf_size = location.size_in_file;
    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) {
        free(p, buf_size);
    });
    buf->readStrict(data_buf, buf_size);
    Page page;
    page.data = ByteBuffer(data_buf, data_buf + buf_size);
    page.mem_holder = mem_holder;
    // Calculate the field_offsets from page entry
    for (size_t index = 0; index < page_entry.field_offsets.size(); index++)
    {
        const auto offset = page_entry.field_offsets[index].first;
        page.field_offsets.emplace(index, offset);
    }
    return page;
}

Page RemotePageReader::read(const PS::V3::RemoteDataLocation & location, const PS::V3::PageEntryV3 & page_entry, std::vector<size_t> fields)
{
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
//    auto buf = std::make_shared<ReadBufferFromFile>(remote_directory + *location.data_file_id);
//    buf->seek(location.offset_in_file);
//    size_t buf_size = 0;
//    for (const auto field_index : fields)
//    {
//        buf_size += page_entry.getFieldSize(field_index);
//    }
//    char * data_buf = static_cast<char *>(alloc(buf_size));
//    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) {
//        free(p, buf_size);
//    });
//    buf->readStrict(data_buf, buf_size);
//    Page page;
//    page.data = ByteBuffer(data_buf, data_buf + buf_size);
//    page.mem_holder = mem_holder;
//    // Calculate the field_offsets from page entry
//    for (size_t index = 0; index < page_entry.field_offsets.size(); index++)
//    {
//        const auto offset = page_entry.field_offsets[index].first;
//        page.field_offsets.emplace(index, offset);
//    }
//
//    std::set<FieldOffsetInsidePage> fields_offset_in_page;
//    for (const auto field_index : fields)
//    {
//        const auto [beg_offset, end_offset] = page_entry.getFieldOffsets(field_index);
//        const auto size_to_read = end_offset - beg_offset;
//        auto blob_file = read(page_id_v3, entry.file_id, entry.offset + beg_offset, write_offset, size_to_read, read_limiter);
//        fields_offset_in_page.emplace(field_index, read_size_this_entry);
//
//        read_size_this_entry += size_to_read;
//        write_offset += size_to_read;
//    }
    return page;
}

}
