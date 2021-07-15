/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 21-5-31
 * Author: huyao
 */
#ifndef CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_
#define CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_

#include <vector>
#include <string>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/error_code.h"

namespace curvefs {
namespace client {

using curvefs::metaserver::S3ChunkInfo;
using curvefs::metaserver::Inode;
using curvefs::metaserver::S3ChunkInfoList;
using curvefs::metaserver::UpdateInodeS3VersionRequest;
using curvefs::metaserver::UpdateInodeS3VersionResponse;


using curvefs::space::AllocateS3ChunkRequest;
using curvefs::space::AllocateS3ChunkResponse;

/*
using namespace curvefs::metaserver;
using namespace curvefs::space;
*/

struct S3ClientAdaptorOption {
    uint64_t blockSize;
    uint64_t chunkSize;
    std::string metaServerEps;
    std::string allocateServerEps;
};

struct S3ReadRequest {
    S3ChunkInfo chunkInfo;
    uint64_t readOffset;
};

struct S3ReadResponse {
    uint64_t readOffset;
    uint64_t bufLen;
    char* dataBuf;
};

// client使用s3存储的内部接口
class S3ClientAdaptor {
 public:
    S3ClientAdaptor() {}
    /**
     * @brief Initailize s3 client
     * @param[in] options the options for s3 client
     */
    void Init(const  S3ClientAdaptorOption  option, S3Client *client);
    /**
     * @brief write data to s3
     * @param[in] options the options for s3 client
     */
    int Write(Inode *inode, uint64_t offset,
              uint64_t length, const char* buf);
    int Read(Inode *inode, uint64_t offset,
              uint64_t length, char* buf);

 private:
    int UpdateInodeS3Version(Inode *inode, uint64_t *version);
    bool IsOverlap(Inode *inode, uint64_t offset, uint64_t length);
    int GetChunkId(Inode *inode, uint64_t index, uint64_t *chunkId);
    CURVEFS_ERROR AllocS3ChunkId(uint32_t fsId, uint64_t *chunkId);
    bool IsAppendBlock(Inode *inode, uint64_t offset, uint64_t length);
    bool IsDiscontinuityInBlock(Inode *inode, uint64_t offset, uint64_t length);
    std::string GenerateObjectName(uint64_t chunkId,
                                   uint64_t blockIndex, uint64_t version);
    uint64_t WriteChunk(uint64_t chunkId, uint64_t version,
                        uint64_t pos, uint64_t length,
                        const char* buf, bool append);
    std::vector<S3ChunkInfo> CutOverLapChunks(const S3ChunkInfo& newChunk,
                                              const S3ChunkInfo& oldChunk);
    std::vector<S3ChunkInfo> GetReadChunks(Inode *inode);
    std::vector<S3ChunkInfo> SortByOffset(std::vector<S3ChunkInfo> chunks);
    int handleReadRequest(std::vector<S3ReadRequest> requests,
                          std::vector<S3ReadResponse>* responses);
    void UpdateInodeChunkInfo(S3ChunkInfoList *s3ChunkInfoList,
                              uint64_t chunkId, uint64_t version,
                              uint64_t offset, uint64_t len);
    S3Client *client_;
    uint64_t blockSize_;
    uint64_t chunkSize_;
    std::string metaServerEps_;
    std::string allocateServerEps_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_
