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

string FileSystemTimeoutRetryWrapper::GetName() const {
	return StringUtil::Format("FileSystemTimeoutRetryWrapper - %s", inner_filesystem->GetName());
}

unique_ptr<FileHandle> FileSystemTimeoutRetryWrapper::OpenFile(const string &path, FileOpenFlags flags,
                                                               optional_ptr<FileOpener> opener) {
	return OpenFileExtended(OpenFileInfo(path), flags, opener);
}

void FileSystemTimeoutRetryWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	inner_filesystem->Read(handle, buffer, nr_bytes, location);
}

void FileSystemTimeoutRetryWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	inner_filesystem->Write(handle, buffer, nr_bytes, location);
}

int64_t FileSystemTimeoutRetryWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	return inner_filesystem->Read(handle, buffer, nr_bytes);
}

int64_t FileSystemTimeoutRetryWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	return inner_filesystem->Write(handle, buffer, nr_bytes);
}
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

bool FileSystemTimeoutRetryWrapper::ListFiles(const string &directory,
                                              const std::function<void(const string &, bool)> &callback,
                                              FileOpener *opener) {
	auto wrapped_callback = [this, &callback](OpenFileInfo &info) {
		const bool is_dir = IsDirectory(info);
		callback(info.path, is_dir);
	};
	return ListFilesExtended(directory, wrapped_callback, opener);
}

unique_ptr<FileHandle> FileSystemTimeoutRetryWrapper::OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
                                                                       optional_ptr<FileOpener> opener) {
	HttpfsOperationType operation_type = HttpfsOperationType::OPEN;
	if (opener) {
		// Check if opener is already a TimeoutRetryFileOpener and preserve its operation type
		auto *existing_timeout_retry_opener = dynamic_cast<TimeoutRetryFileOpener *>(opener.get());
		if (existing_timeout_retry_opener) {
			// Use the existing opener directly since it's already configured
			return inner_filesystem->OpenFile(path, flags, opener);
		}
		TimeoutRetryFileOpener wrapped_opener(*opener, operation_type);
		return inner_filesystem->OpenFile(path, flags, &wrapped_opener);
	}
	DatabaseFileOpener database_opener(db);
	TimeoutRetryFileOpener wrapped_opener(database_opener, operation_type);
	return inner_filesystem->OpenFile(path, flags, &wrapped_opener);
}

bool FileSystemTimeoutRetryWrapper::SupportsOpenFileExtended() const {
	return true;
}

bool FileSystemTimeoutRetryWrapper::ListFilesExtended(const string &directory,
                                                      const std::function<void(OpenFileInfo &info)> &callback,
                                                      optional_ptr<FileOpener> opener) {
	if (opener) {
		TimeoutRetryFileOpener timeout_retry_opener(*opener, HttpfsOperationType::LIST);
		return inner_filesystem->ListFiles(directory, callback, &timeout_retry_opener);
	}
	DatabaseFileOpener database_opener(db);
	TimeoutRetryFileOpener timeout_retry_opener(database_opener, HttpfsOperationType::LIST);
	return inner_filesystem->ListFiles(directory, callback, &timeout_retry_opener);
}

bool FileSystemTimeoutRetryWrapper::SupportsListFilesExtended() const {
	return true;
}

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

void FileSystemTimeoutRetryWrapper::Seek(FileHandle &handle, idx_t location) {
	inner_filesystem->Seek(handle, location);
}

void FileSystemTimeoutRetryWrapper::Reset(FileHandle &handle) {
	inner_filesystem->Reset(handle);
}

idx_t FileSystemTimeoutRetryWrapper::SeekPosition(FileHandle &handle) {
	return inner_filesystem->SeekPosition(handle);
}

bool FileSystemTimeoutRetryWrapper::IsManuallySet() {
	return inner_filesystem->IsManuallySet();
}

bool FileSystemTimeoutRetryWrapper::CanSeek() {
	return inner_filesystem->CanSeek();
}

bool FileSystemTimeoutRetryWrapper::OnDiskFile(FileHandle &handle) {
	return inner_filesystem->OnDiskFile(handle);
}

unique_ptr<FileHandle> FileSystemTimeoutRetryWrapper::OpenCompressedFile(QueryContext context,
                                                                         unique_ptr<FileHandle> handle, bool write) {
	return inner_filesystem->OpenCompressedFile(context, std::move(handle), write);
}

void FileSystemTimeoutRetryWrapper::SetDisabledFileSystems(const vector<string> &names) {
	inner_filesystem->SetDisabledFileSystems(names);
}

bool FileSystemTimeoutRetryWrapper::SubSystemIsDisabled(const string &name) {
	return inner_filesystem->SubSystemIsDisabled(name);
}

} // namespace duckdb
