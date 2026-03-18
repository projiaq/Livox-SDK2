//
// The MIT License (MIT)
//
// Copyright (c) 2022 Livox. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

#include "livox_lidar_api.h"
#include "livox_lidar_def.h"
#include "data_handler/data_handler.h"

#ifdef _WIN32
#include <winsock2.h>
#include <direct.h>
#include <sys/stat.h>
#else
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <condition_variable>
#include <deque>
#include <ctime>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> g_exit_requested(false);

uint64_t GetCurrentTimeNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());
}

bool ConvertToLocalTime(std::time_t timestamp, std::tm* out_tm) {
  if (out_tm == nullptr) {
    return false;
  }
#ifdef _WIN32
  return localtime_s(out_tm, &timestamp) == 0;
#else
  return localtime_r(&timestamp, out_tm) != nullptr;
#endif
}

std::string FormatMinuteString(std::time_t minute_start) {
  std::tm local_tm;
  if (!ConvertToLocalTime(minute_start, &local_tm)) {
    return "unknown_time";
  }

  char buffer[32];
  std::strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M", &local_tm);
  return buffer;
}

std::string NormalizePath(std::string path) {
  for (size_t i = 0; i < path.size(); ++i) {
    if (path[i] == '\\') {
      path[i] = '/';
    }
  }

  while (path.size() > 1 && path[path.size() - 1] == '/') {
    path.erase(path.size() - 1);
  }
  return path;
}

std::string JoinPath(const std::string& left, const std::string& right) {
  if (left.empty()) {
    return right;
  }
  if (right.empty()) {
    return left;
  }
  if (left[left.size() - 1] == '/') {
    return left + right;
  }
  return left + "/" + right;
}

bool DirectoryExists(const std::string& path) {
  if (path.empty()) {
    return false;
  }

#ifdef _WIN32
  struct _stat info;
  if (_stat(path.c_str(), &info) != 0) {
    return false;
  }
  return (info.st_mode & _S_IFDIR) != 0;
#else
  struct stat info;
  if (stat(path.c_str(), &info) != 0) {
    return false;
  }
  return S_ISDIR(info.st_mode);
#endif
}

bool MakeDirectoryIfMissing(const std::string& path) {
  if (path.empty() || DirectoryExists(path)) {
    return true;
  }

#ifdef _WIN32
  if (_mkdir(path.c_str()) == 0) {
    return true;
  }
#else
  if (mkdir(path.c_str(), 0755) == 0) {
    return true;
  }
#endif

  return errno == EEXIST && DirectoryExists(path);
}

bool EnsureDirectory(const std::string& raw_path) {
  const std::string path = NormalizePath(raw_path);
  if (path.empty()) {
    return false;
  }

  if (DirectoryExists(path)) {
    return true;
  }

  std::string current;
  size_t index = 0;

#ifdef _WIN32
  if (path.size() >= 2 && path[1] == ':') {
    current = path.substr(0, 2);
    index = 2;
  }
#endif

  if (index < path.size() && path[index] == '/') {
    current += "/";
    ++index;
  }

  while (index <= path.size()) {
    const size_t next = path.find('/', index);
    const std::string part = path.substr(index, next - index);
    if (!part.empty()) {
      if (!current.empty() && current[current.size() - 1] != '/') {
        current += "/";
      }
      current += part;
      if (!MakeDirectoryIfMissing(current)) {
        return false;
      }
    }

    if (next == std::string::npos) {
      break;
    }
    index = next + 1;
  }

  return DirectoryExists(path);
}

std::string SanitizeFileName(std::string text) {
  for (size_t i = 0; i < text.size(); ++i) {
    const char ch = text[i];
    const bool is_digit = ch >= '0' && ch <= '9';
    const bool is_lower = ch >= 'a' && ch <= 'z';
    const bool is_upper = ch >= 'A' && ch <= 'Z';
    if (is_digit || is_lower || is_upper || ch == '-' || ch == '_') {
      continue;
    }
    text[i] = '_';
  }

  if (text.empty()) {
    return "unknown";
  }
  return text;
}

std::string HandleToIpString(uint32_t handle) {
  const uint32_t ip_host_order = ntohl(handle);
  std::ostringstream oss;
  oss << ((ip_host_order >> 24) & 0xFF) << "."
      << ((ip_host_order >> 16) & 0xFF) << "."
      << ((ip_host_order >> 8) & 0xFF) << "."
      << (ip_host_order & 0xFF);
  return oss.str();
}

uint64_t ParseLidarTimestamp(const uint8_t timestamp[8]) {
  uint64_t value = 0;
  std::memcpy(&value, timestamp, sizeof(value));
  return value;
}

size_t GetPointSize(uint8_t data_type) {
  if (data_type == kLivoxLidarCartesianCoordinateHighData) {
    return sizeof(LivoxLidarCartesianHighRawPoint);
  }
  if (data_type == kLivoxLidarCartesianCoordinateLowData) {
    return sizeof(LivoxLidarCartesianLowRawPoint);
  }
  if (data_type == kLivoxLidarSphericalCoordinateData) {
    return sizeof(LivoxLidarSpherPoint);
  }
  return 0;
}

void SleepForMilliseconds(uint32_t milliseconds) {
#ifdef _WIN32
  Sleep(milliseconds);
#else
  usleep(milliseconds * 1000);
#endif
}

#pragma pack(push, 1)
struct BinRecordHeader {
  char magic[8];
  uint32_t header_size;
  uint32_t packet_size;
  uint32_t handle;
  uint8_t dev_type;
  uint8_t data_type;
  uint16_t dot_num;
  uint16_t udp_cnt;
  uint8_t frame_cnt;
  uint8_t time_type;
  uint64_t host_timestamp_ns;
  uint64_t lidar_timestamp;
};
#pragma pack(pop)

struct DeviceMeta {
  std::string sn;
  std::string lidar_ip;
  uint8_t dev_type;
};

struct PacketFrame {
  uint32_t handle;
  uint8_t dev_type;
  uint64_t host_timestamp_ns;
  uint64_t lidar_timestamp;
  uint32_t recv_size;
  std::vector<uint8_t> raw_packet;
};

struct OutputWriters {
  std::time_t minute_start;
  std::ofstream bin_stream;
  std::ofstream csv_stream;

  OutputWriters() : minute_start(0) {}
};

class PointCloudRecorder {
 public:
  explicit PointCloudRecorder(const std::string& output_dir)
      : output_root_(NormalizePath(output_dir)),
        bin_root_(JoinPath(output_root_, "bin")),
        csv_root_(JoinPath(output_root_, "csv")),
        stop_requested_(false) {}

  bool Init() {
    if (!EnsureDirectory(output_root_)) {
      std::printf("Create output directory failed: %s\n", output_root_.c_str());
      return false;
    }
    if (!EnsureDirectory(bin_root_)) {
      std::printf("Create bin directory failed: %s\n", bin_root_.c_str());
      return false;
    }
    if (!EnsureDirectory(csv_root_)) {
      std::printf("Create csv directory failed: %s\n", csv_root_.c_str());
      return false;
    }

    worker_thread_ = std::thread(&PointCloudRecorder::Run, this);
    return true;
  }

  void Stop() {
    stop_requested_.store(true);
    queue_cv_.notify_one();
    if (worker_thread_.joinable()) {
      worker_thread_.join();
    }
    CloseAllWriters();
  }

  void UpdateDeviceMeta(uint32_t handle, const LivoxLidarInfo* info) {
    if (info == nullptr) {
      return;
    }

    DeviceMeta meta;
    meta.sn = info->sn;
    meta.lidar_ip = info->lidar_ip;
    meta.dev_type = info->dev_type;

    std::lock_guard<std::mutex> lock(meta_mutex_);
    device_meta_[handle] = meta;
  }

  void PushPointPacket(uint32_t handle, uint8_t dev_type, LivoxLidarEthernetPacket* packet, uint32_t raw_size) {
    if (packet == nullptr || packet->data_type == kLivoxLidarImuData) {
      return;
    }

    const size_t packet_size = static_cast<size_t>(raw_size);
    const size_t header_size = offsetof(LivoxLidarEthernetPacket, data);
    const size_t point_size = GetPointSize(packet->data_type);

    if (packet_size < header_size || point_size == 0) {
      return;
    }

    PacketFrame frame;
    frame.handle = handle;
    frame.dev_type = dev_type;
    frame.host_timestamp_ns = GetCurrentTimeNs();
    frame.lidar_timestamp = ParseLidarTimestamp(packet->timestamp);
    frame.recv_size = raw_size;
    frame.raw_packet.resize(packet_size);
    std::memcpy(frame.raw_packet.data(), packet, packet_size);

    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      packet_queue_.push_back(std::move(frame));
    }
    queue_cv_.notify_one();
  }

 private:
  void Run() {
    while (true) {
      PacketFrame frame;
      {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this] {
          return stop_requested_.load() || !packet_queue_.empty();
        });

        if (packet_queue_.empty()) {
          if (stop_requested_.load()) {
            break;
          }
          continue;
        }

        frame = std::move(packet_queue_.front());
        packet_queue_.pop_front();
      }

      WritePacket(frame);
    }
  }

  void WritePacket(const PacketFrame& frame) {
    if (frame.raw_packet.empty()) {
      return;
    }

    const LivoxLidarEthernetPacket* packet =
        reinterpret_cast<const LivoxLidarEthernetPacket*>(frame.raw_packet.data());

    DeviceMeta meta = GetDeviceMeta(frame.handle, frame.dev_type);
    OutputWriters& writers = GetWriters(frame.handle, frame.host_timestamp_ns, meta);

    WriteBinRecord(writers.bin_stream, frame, packet);
    WriteCsvRows(writers.csv_stream, frame, meta, packet);
  }

  DeviceMeta GetDeviceMeta(uint32_t handle, uint8_t dev_type) {
    std::lock_guard<std::mutex> lock(meta_mutex_);
    std::map<uint32_t, DeviceMeta>::const_iterator it = device_meta_.find(handle);
    if (it != device_meta_.end()) {
      return it->second;
    }

    DeviceMeta meta;
    meta.sn = "unknown_sn";
    meta.lidar_ip = HandleToIpString(handle);
    meta.dev_type = dev_type;
    return meta;
  }

  OutputWriters& GetWriters(uint32_t handle, uint64_t host_timestamp_ns, const DeviceMeta& meta) {
    const std::time_t second_time = static_cast<std::time_t>(host_timestamp_ns / 1000000000ULL);
    const std::time_t minute_start = second_time - (second_time % 60);

    OutputWriters& writers = writers_[handle];
    if (writers.bin_stream.is_open() && writers.csv_stream.is_open() &&
        writers.minute_start == minute_start) {
      return writers;
    }

    if (writers.bin_stream.is_open()) {
      writers.bin_stream.close();
    }
    if (writers.csv_stream.is_open()) {
      writers.csv_stream.close();
    }

    writers.minute_start = minute_start;

    const std::string file_prefix = BuildFilePrefix(meta, minute_start);
    const std::string bin_path = JoinPath(bin_root_, file_prefix + ".bin");
    const std::string csv_path = JoinPath(csv_root_, file_prefix + ".csv");

    writers.bin_stream.open(bin_path.c_str(), std::ios::binary | std::ios::out);
    writers.csv_stream.open(csv_path.c_str(), std::ios::out);

    if (writers.csv_stream.is_open()) {
      writers.csv_stream
          << "host_timestamp_ns,lidar_timestamp,recv_size,handle,lidar_ip,sn,dev_type,data_type,"
          << "frame_cnt,udp_cnt,point_index,x,y,z,depth,theta,phi,reflectivity,tag\n";
    }

    return writers;
  }

  std::string BuildFilePrefix(const DeviceMeta& meta, std::time_t minute_start) const {
    const std::string minute_text = FormatMinuteString(minute_start);
    const std::string sn = SanitizeFileName(meta.sn);
    const std::string ip = SanitizeFileName(meta.lidar_ip);

    std::ostringstream oss;
    oss << "lidar_" << sn << "_" << ip << "_" << minute_text;
    return oss.str();
  }

  void WriteBinRecord(std::ofstream& stream,
                      const PacketFrame& frame,
                      const LivoxLidarEthernetPacket* packet) {
    if (!stream.is_open() || packet == nullptr) {
      return;
    }

    BinRecordHeader header;
    std::memset(&header, 0, sizeof(header));
    std::memcpy(header.magic, "LIVOXPC", 7);
    header.header_size = static_cast<uint32_t>(sizeof(header));
    header.packet_size = frame.recv_size;
    header.handle = frame.handle;
    header.dev_type = frame.dev_type;
    header.data_type = packet->data_type;
    header.dot_num = packet->dot_num;
    header.udp_cnt = packet->udp_cnt;
    header.frame_cnt = packet->frame_cnt;
    header.time_type = packet->time_type;
    header.host_timestamp_ns = frame.host_timestamp_ns;
    header.lidar_timestamp = frame.lidar_timestamp;

    stream.write(reinterpret_cast<const char*>(&header), sizeof(header));
    stream.write(reinterpret_cast<const char*>(frame.raw_packet.data()),
                 static_cast<std::streamsize>(frame.raw_packet.size()));
  }

  void WriteCsvRows(std::ofstream& stream,
                    const PacketFrame& frame,
                    const DeviceMeta& meta,
                    const LivoxLidarEthernetPacket* packet) {
    if (!stream.is_open() || packet == nullptr) {
      return;
    }

    const size_t header_size = offsetof(LivoxLidarEthernetPacket, data);
    const size_t point_size = GetPointSize(packet->data_type);
    const size_t declared_size = static_cast<size_t>(packet->length);
    const size_t point_area_size = point_size * static_cast<size_t>(packet->dot_num);
    if (point_size == 0 || declared_size < header_size || frame.raw_packet.size() < declared_size ||
        declared_size < header_size + point_area_size) {
      return;
    }

    const uint8_t* point_data = packet->data;
    if (packet->data_type == kLivoxLidarCartesianCoordinateHighData) {
      const LivoxLidarCartesianHighRawPoint* points =
          reinterpret_cast<const LivoxLidarCartesianHighRawPoint*>(point_data);
      for (uint32_t i = 0; i < packet->dot_num; ++i) {
        stream << frame.host_timestamp_ns << ','
               << frame.lidar_timestamp << ','
               << frame.recv_size << ','
               << frame.handle << ','
               << meta.lidar_ip << ','
               << meta.sn << ','
               << static_cast<uint32_t>(frame.dev_type) << ','
               << static_cast<uint32_t>(packet->data_type) << ','
               << static_cast<uint32_t>(packet->frame_cnt) << ','
               << packet->udp_cnt << ','
               << i << ','
               << points[i].x << ','
               << points[i].y << ','
               << points[i].z << ','
               << ','
               << ','
               << ','
               << static_cast<uint32_t>(points[i].reflectivity) << ','
               << static_cast<uint32_t>(points[i].tag) << '\n';
      }
      return;
    }

    if (packet->data_type == kLivoxLidarCartesianCoordinateLowData) {
      const LivoxLidarCartesianLowRawPoint* points =
          reinterpret_cast<const LivoxLidarCartesianLowRawPoint*>(point_data);
      for (uint32_t i = 0; i < packet->dot_num; ++i) {
        stream << frame.host_timestamp_ns << ','
               << frame.lidar_timestamp << ','
               << frame.recv_size << ','
               << frame.handle << ','
               << meta.lidar_ip << ','
               << meta.sn << ','
               << static_cast<uint32_t>(frame.dev_type) << ','
               << static_cast<uint32_t>(packet->data_type) << ','
               << static_cast<uint32_t>(packet->frame_cnt) << ','
               << packet->udp_cnt << ','
               << i << ','
               << points[i].x << ','
               << points[i].y << ','
               << points[i].z << ','
               << ','
               << ','
               << ','
               << static_cast<uint32_t>(points[i].reflectivity) << ','
               << static_cast<uint32_t>(points[i].tag) << '\n';
      }
      return;
    }

    if (packet->data_type == kLivoxLidarSphericalCoordinateData) {
      const LivoxLidarSpherPoint* points =
          reinterpret_cast<const LivoxLidarSpherPoint*>(point_data);
      for (uint32_t i = 0; i < packet->dot_num; ++i) {
        stream << frame.host_timestamp_ns << ','
               << frame.lidar_timestamp << ','
               << frame.recv_size << ','
               << frame.handle << ','
               << meta.lidar_ip << ','
               << meta.sn << ','
               << static_cast<uint32_t>(frame.dev_type) << ','
               << static_cast<uint32_t>(packet->data_type) << ','
               << static_cast<uint32_t>(packet->frame_cnt) << ','
               << packet->udp_cnt << ','
               << i << ','
               << ','
               << ','
               << ','
               << points[i].depth << ','
               << points[i].theta << ','
               << points[i].phi << ','
               << static_cast<uint32_t>(points[i].reflectivity) << ','
               << static_cast<uint32_t>(points[i].tag) << '\n';
      }
    }
  }

  void CloseAllWriters() {
    for (std::map<uint32_t, OutputWriters>::iterator it = writers_.begin();
         it != writers_.end(); ++it) {
      if (it->second.bin_stream.is_open()) {
        it->second.bin_stream.close();
      }
      if (it->second.csv_stream.is_open()) {
        it->second.csv_stream.close();
      }
    }
    writers_.clear();
  }

 private:
  std::string output_root_;
  std::string bin_root_;
  std::string csv_root_;

  std::atomic<bool> stop_requested_;
  std::thread worker_thread_;

  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::deque<PacketFrame> packet_queue_;

  std::mutex meta_mutex_;
  std::map<uint32_t, DeviceMeta> device_meta_;
  std::map<uint32_t, OutputWriters> writers_;
};

void WorkModeCallback(livox_status status,
                      uint32_t handle,
                      LivoxLidarAsyncControlResponse* response,
                      void* client_data) {
  if (response == nullptr) {
    return;
  }

  std::printf("Set work mode, status:%d handle:%u ret_code:%u error_key:%u\n",
              status,
              handle,
              response->ret_code,
              response->error_key);
}

void PointCloudCallback(uint32_t handle,
                        const uint8_t dev_type,
                        LivoxLidarEthernetPacket* data,
                        uint32_t data_size,
                        void* client_data) {
  PointCloudRecorder* recorder = reinterpret_cast<PointCloudRecorder*>(client_data);
  if (recorder == nullptr || data == nullptr) {
    return;
  }
  recorder->PushPointPacket(handle, dev_type, data, data_size);
}

void LidarInfoChangeCallback(const uint32_t handle,
                             const LivoxLidarInfo* info,
                             void* client_data) {
  PointCloudRecorder* recorder = reinterpret_cast<PointCloudRecorder*>(client_data);
  if (recorder == nullptr || info == nullptr) {
    return;
  }

  recorder->UpdateDeviceMeta(handle, info);
  std::printf("Detected lidar handle:%u sn:%s ip:%s dev_type:%u\n",
              handle,
              info->sn,
              info->lidar_ip,
              static_cast<uint32_t>(info->dev_type));

  SetLivoxLidarWorkMode(handle, kLivoxLidarNormal, WorkModeCallback, nullptr);
}

void StopSignalHandler(int signal) {
  (void)signal;
  g_exit_requested.store(true);
}

}  // namespace

int main(int argc, const char* argv[]) {
  if (argc != 3) {
    std::printf("Usage: point_cloud_recorder <config_path> <output_dir>\n");
    return -1;
  }

  const std::string config_path = argv[1];
  const std::string output_dir = argv[2];

  PointCloudRecorder recorder(output_dir);
  if (!recorder.Init()) {
    return -1;
  }

  if (!LivoxLidarSdkInit(config_path.c_str())) {
    std::printf("Livox Init Failed\n");
    recorder.Stop();
    LivoxLidarSdkUninit();
    return -1;
  }

  livox::lidar::DataHandler::GetInstance().SetRawPointDataCallback(PointCloudCallback, &recorder);
  SetLivoxLidarInfoChangeCallback(LidarInfoChangeCallback, &recorder);

  std::signal(SIGINT, StopSignalHandler);

  while (!g_exit_requested.load()) {
    SleepForMilliseconds(200);
  }

  LivoxLidarSdkUninit();
  recorder.Stop();
  std::printf("Point cloud recorder exit.\n");
  return 0;
}
