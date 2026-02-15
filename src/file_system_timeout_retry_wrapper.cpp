#include "file_system_timeout_retry_wrapper.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "timeout_retry_file_opener.hpp"

namespace duckdb {

FileSystemTimeoutRetryWrapper::FileSystemTimeoutRetryWrapper(unique_ptr<FileSystem> inner_filesystem,
                                                              DatabaseInstance &db)
    : inner_filesystem(std::move(inner_filesystem)), db(db) {
}

std::string FileSystemTimeoutRetryWrapper::GetName() const {
	return StringUtil::Format("FileSystemTimeoutRetryWrapper - %s", inner_filesystem->GetName());
}

//===--------------------------------------------------------------------===//
// IO operations
//===--------------------------------------------------------------------===//

// Open operations
unique_ptr<FileHandle> FileSystemTimeoutRetryWrapper::OpenFile(const string &path, FileOpenFlags flags,
                                                                optional_ptr<FileOpener> opener) {
	if (opener) {
		TimeoutRetryFileOpener timeout_retry_opener(*opener, HttpfsOperationType::OPEN);
		return inner_filesystem->OpenFile(path, flags, &timeout_retry_opener);
	}
	// When no opener is provided, create a DatabaseFileOpener to read settings from database config
	DatabaseFileOpener database_opener(db);
	TimeoutRetryFileOpener timeout_retry_opener(database_opener, HttpfsOperationType::OPEN);
	return inner_filesystem->OpenFile(path, flags, &timeout_retry_opener);
}

unique_ptr<FileHandle> FileSystemTimeoutRetryWrapper::OpenFile(const OpenFileInfo &path, FileOpenFlags flags,
                                                                optional_ptr<FileOpener> opener) {
	if (opener) {
		TimeoutRetryFileOpener timeout_retry_opener(*opener, HttpfsOperationType::OPEN);
		return inner_filesystem->OpenFile(path, flags, &timeout_retry_opener);
	}
	// When no opener is provided, create a DatabaseFileOpener to read settings from database config
	DatabaseFileOpener database_opener(db);
	TimeoutRetryFileOpener timeout_retry_opener(database_opener, HttpfsOperationType::OPEN);
	return inner_filesystem->OpenFile(path, flags, &timeout_retry_opener);
}

// Read/Write operations
void FileSystemTimeoutRetryWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	// Note: Read/Write operations use the FileHandle which was created with timeout settings during OpenFile
	// The timeout for read operations is set when the handle is opened, so we delegate directly
	inner_filesystem->Read(handle, buffer, nr_bytes, location);
}

void FileSystemTimeoutRetryWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	// Note: Read/Write operations use the FileHandle which was created with timeout settings during OpenFile
	// The timeout for write operations is set when the handle is opened, so we delegate directly
	inner_filesystem->Write(handle, buffer, nr_bytes, location);
}

int64_t FileSystemTimeoutRetryWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	// Note: Read/Write operations use the FileHandle which was created with timeout settings during OpenFile
	// The timeout for read operations is set when the handle is opened, so we delegate directly
	return inner_filesystem->Read(handle, buffer, nr_bytes);
}

int64_t FileSystemTimeoutRetryWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	// Note: Read/Write operations use the FileHandle which was created with timeout settings during OpenFile
	// The timeout for write operations is set when the handle is opened, so we delegate directly
	return inner_filesystem->Write(handle, buffer, nr_bytes);
}

// File info operations
int64_t FileSystemTimeoutRetryWrapper::GetFileSize(FileHandle &handle) {
	return inner_filesystem->GetFileSize(handle);
}

timestamp_t FileSystemTimeoutRetryWrapper::GetLastModifiedTime(FileHandle &handle) {
	return inner_filesystem->GetLastModifiedTime(handle);
}

string FileSystemTimeoutRetryWrapper::GetVersionTag(FileHandle &handle) {
	return inner_filesystem->GetVersionTag(handle);
}

FileType FileSystemTimeoutRetryWrapper::GetFileType(FileHandle &handle) {
	return inner_filesystem->GetFileType(handle);
}

// Directory operations
bool FileSystemTimeoutRetryWrapper::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return inner_filesystem->DirectoryExists(directory, opener);
}

void FileSystemTimeoutRetryWrapper::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	inner_filesystem->CreateDirectory(directory, opener);
}

void FileSystemTimeoutRetryWrapper::CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener) {
	inner_filesystem->CreateDirectoriesRecursive(path, opener);
}

void FileSystemTimeoutRetryWrapper::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	inner_filesystem->RemoveDirectory(directory, opener);
}

// List operations
bool FileSystemTimeoutRetryWrapper::ListFiles(const string &directory,
                                              const std::function<void(const string &, bool)> &callback,
                                              FileOpener *opener) {
	if (opener) {
		TimeoutRetryFileOpener timeout_retry_opener(*opener, HttpfsOperationType::LIST);
		return inner_filesystem->ListFiles(directory, callback, &timeout_retry_opener);
	}
	// When no opener is provided, create a DatabaseFileOpener to read settings from database config
	DatabaseFileOpener database_opener(db);
	TimeoutRetryFileOpener timeout_retry_opener(database_opener, HttpfsOperationType::LIST);
	return inner_filesystem->ListFiles(directory, callback, &timeout_retry_opener);
}

bool FileSystemTimeoutRetryWrapper::ListFiles(const string &directory,
                                              const std::function<void(OpenFileInfo &info)> &callback,
                                              optional_ptr<FileOpener> opener) {
	if (opener) {
		TimeoutRetryFileOpener timeout_retry_opener(*opener, HttpfsOperationType::LIST);
		return inner_filesystem->ListFiles(directory, callback, &timeout_retry_opener);
	}
	// When no opener is provided, create a DatabaseFileOpener to read settings from database config
	DatabaseFileOpener database_opener(db);
	TimeoutRetryFileOpener timeout_retry_opener(database_opener, HttpfsOperationType::LIST);
	return inner_filesystem->ListFiles(directory, callback, &timeout_retry_opener);
}

unique_ptr<FileHandle> FileSystemTimeoutRetryWrapper::OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
                                                                       optional_ptr<FileOpener> opener) {
	if (opener) {
		TimeoutRetryFileOpener timeout_retry_opener(*opener, HttpfsOperationType::OPEN);
		return inner_filesystem->OpenFileExtended(path, flags, &timeout_retry_opener);
	}
	// When no opener is provided, create a DatabaseFileOpener to read settings from database config
	DatabaseFileOpener database_opener(db);
	TimeoutRetryFileOpener timeout_retry_opener(database_opener, HttpfsOperationType::OPEN);
	return inner_filesystem->OpenFileExtended(path, flags, &timeout_retry_opener);
}

bool FileSystemTimeoutRetryWrapper::SupportsOpenFileExtended() const {
	return inner_filesystem->SupportsOpenFileExtended();
}

bool FileSystemTimeoutRetryWrapper::ListFilesExtended(const string &directory,
                                                       const std::function<void(OpenFileInfo &info)> &callback,
                                                       optional_ptr<FileOpener> opener) {
	if (opener) {
		TimeoutRetryFileOpener timeout_retry_opener(*opener, HttpfsOperationType::LIST);
		return inner_filesystem->ListFilesExtended(directory, callback, &timeout_retry_opener);
	}
	// When no opener is provided, create a DatabaseFileOpener to read settings from database config
	DatabaseFileOpener database_opener(db);
	TimeoutRetryFileOpener timeout_retry_opener(database_opener, HttpfsOperationType::LIST);
	return inner_filesystem->ListFilesExtended(directory, callback, &timeout_retry_opener);
}

bool FileSystemTimeoutRetryWrapper::SupportsListFilesExtended() const {
	return inner_filesystem->SupportsListFilesExtended();
}

//===--------------------------------------------------------------------===//
// Non-IO operations
//===--------------------------------------------------------------------===//

void FileSystemTimeoutRetryWrapper::MoveFile(const string &source, const string &target,
                                             optional_ptr<FileOpener> opener) {
	inner_filesystem->MoveFile(source, target, opener);
}

bool FileSystemTimeoutRetryWrapper::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return inner_filesystem->FileExists(filename, opener);
}

bool FileSystemTimeoutRetryWrapper::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return inner_filesystem->IsPipe(filename, opener);
}

void FileSystemTimeoutRetryWrapper::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	if (opener) {
		TimeoutRetryFileOpener timeout_retry_opener(*opener, HttpfsOperationType::DELETE);
		inner_filesystem->RemoveFile(filename, &timeout_retry_opener);
	} else {
		// When no opener is provided, create a DatabaseFileOpener to read settings from database config
		DatabaseFileOpener database_opener(db);
		TimeoutRetryFileOpener timeout_retry_opener(database_opener, HttpfsOperationType::DELETE);
		inner_filesystem->RemoveFile(filename, &timeout_retry_opener);
	}
}

bool FileSystemTimeoutRetryWrapper::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	if (opener) {
		TimeoutRetryFileOpener timeout_retry_opener(*opener, HttpfsOperationType::DELETE);
		return inner_filesystem->TryRemoveFile(filename, &timeout_retry_opener);
	}
	// When no opener is provided, create a DatabaseFileOpener to read settings from database config
	DatabaseFileOpener database_opener(db);
	TimeoutRetryFileOpener timeout_retry_opener(database_opener, HttpfsOperationType::DELETE);
	return inner_filesystem->TryRemoveFile(filename, &timeout_retry_opener);
}

void FileSystemTimeoutRetryWrapper::FileSync(FileHandle &handle) {
	inner_filesystem->FileSync(handle);
}

void FileSystemTimeoutRetryWrapper::Truncate(FileHandle &handle, int64_t new_size) {
	inner_filesystem->Truncate(handle, new_size);
}

bool FileSystemTimeoutRetryWrapper::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	return inner_filesystem->Trim(handle, offset_bytes, length_bytes);
}

string FileSystemTimeoutRetryWrapper::GetHomeDirectory() {
	return inner_filesystem->GetHomeDirectory();
}

string FileSystemTimeoutRetryWrapper::ExpandPath(const string &path) {
	return inner_filesystem->ExpandPath(path);
}

string FileSystemTimeoutRetryWrapper::PathSeparator(const string &path) {
	return inner_filesystem->PathSeparator(path);
}

vector<OpenFileInfo> FileSystemTimeoutRetryWrapper::Glob(const string &path, FileOpener *opener) {
	return inner_filesystem->Glob(path, opener);
}

void FileSystemTimeoutRetryWrapper::RegisterSubSystem(unique_ptr<FileSystem> sub_fs) {
	inner_filesystem->RegisterSubSystem(std::move(sub_fs));
}

void FileSystemTimeoutRetryWrapper::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) {
	inner_filesystem->RegisterSubSystem(compression_type, std::move(fs));
}

void FileSystemTimeoutRetryWrapper::UnregisterSubSystem(const string &name) {
	inner_filesystem->UnregisterSubSystem(name);
}

unique_ptr<FileSystem> FileSystemTimeoutRetryWrapper::ExtractSubSystem(const string &name) {
	return inner_filesystem->ExtractSubSystem(name);
}

vector<string> FileSystemTimeoutRetryWrapper::ListSubSystems() {
	return inner_filesystem->ListSubSystems();
}

bool FileSystemTimeoutRetryWrapper::CanHandleFile(const string &fpath) {
	return inner_filesystem->CanHandleFile(fpath);
}

// Seek operations
void FileSystemTimeoutRetryWrapper::Seek(FileHandle &handle, idx_t location) {
	inner_filesystem->Seek(handle, location);
}

void FileSystemTimeoutRetryWrapper::Reset(FileHandle &handle) {
	inner_filesystem->Reset(handle);
}

idx_t FileSystemTimeoutRetryWrapper::SeekPosition(FileHandle &handle) {
	return inner_filesystem->SeekPosition(handle);
}

// File system properties
bool FileSystemTimeoutRetryWrapper::IsManuallySet() {
	return inner_filesystem->IsManuallySet();
}

bool FileSystemTimeoutRetryWrapper::CanSeek() {
	return inner_filesystem->CanSeek();
}

bool FileSystemTimeoutRetryWrapper::OnDiskFile(FileHandle &handle) {
	return inner_filesystem->OnDiskFile(handle);
}

// Compressed file operations
unique_ptr<FileHandle> FileSystemTimeoutRetryWrapper::OpenCompressedFile(QueryContext context,
                                                                         unique_ptr<FileHandle> handle, bool write) {
	return inner_filesystem->OpenCompressedFile(context, std::move(handle), write);
}

// Disabled filesystem operations
void FileSystemTimeoutRetryWrapper::SetDisabledFileSystems(const vector<string> &names) {
	inner_filesystem->SetDisabledFileSystems(names);
}

bool FileSystemTimeoutRetryWrapper::SubSystemIsDisabled(const string &name) {
	return inner_filesystem->SubSystemIsDisabled(name);
}

} // namespace duckdb
