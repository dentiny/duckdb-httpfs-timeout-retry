#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

// FileSystemTimeoutRetryWrapper wraps a filesystem and adds timeout and retry logic
// for specific IO operations (open, list, delete, etc.)
class FileSystemTimeoutRetryWrapper : public FileSystem {
public:
	FileSystemTimeoutRetryWrapper(unique_ptr<FileSystem> inner_filesystem, DatabaseInstance &db);

	string GetName() const override;

	//===--------------------------------------------------------------------===//
	// IO operations
	//===--------------------------------------------------------------------===//

	// Open operations
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	// Read/Write operations
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	// File info operations
	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;

	// Directory operations
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;

	// List operations
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;

protected:
	// Extended IO operations
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;
	bool SupportsOpenFileExtended() const override;

	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;
	bool SupportsListFilesExtended() const override;

	//===--------------------------------------------------------------------===//
	// Non-IO operations
	//===--------------------------------------------------------------------===//

public:
	// File management operations
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void FileSync(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;

	// Path operations
	string GetHomeDirectory() override;
	string ExpandPath(const string &path) override;
	string PathSeparator(const string &path) override;

	// Glob operations
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	// Subsystem operations
	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override;
	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override;
	void UnregisterSubSystem(const string &name) override;
	unique_ptr<FileSystem> ExtractSubSystem(const string &name) override;
	vector<string> ListSubSystems() override;
	bool CanHandleFile(const string &fpath) override;

	// Seek operations
	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;

	// File system properties
	bool IsManuallySet() override;
	bool CanSeek() override;
	bool OnDiskFile(FileHandle &handle) override;

	// Compressed file operations
	unique_ptr<FileHandle> OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle, bool write) override;

	// Disabled filesystem operations
	void SetDisabledFileSystems(const vector<string> &names) override;
	bool SubSystemIsDisabled(const string &name) override;

private:
	unique_ptr<FileSystem> inner_filesystem;
	DatabaseInstance &db;
};

} // namespace duckdb
