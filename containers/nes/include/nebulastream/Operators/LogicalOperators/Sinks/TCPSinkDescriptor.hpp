/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_TCPSINKDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_TCPSINKDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <cstdint>
#include <string>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating a TCP sink (MessagePack-over-TCP).
 */
class TCPSinkDescriptor : public SinkDescriptor {
  public:
    static SinkDescriptorPtr create(std::string host, uint16_t port, uint64_t numberOfOrigins = 1);

    const std::string& getHost() const;
    uint16_t getPort() const;

    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;
    std::string toString() const override;

  private:
    explicit TCPSinkDescriptor(std::string host, uint16_t port, uint64_t numberOfOrigins);

    std::string host;
    uint16_t port;
};

using TCPSinkDescriptorPtr = std::shared_ptr<TCPSinkDescriptor>;

}// namespace NES

// DSL-friendly alias to match typical naming in query snippets
namespace NES {
using TcpSinkDescriptor = TCPSinkDescriptor;
}

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_TCPSINKDESCRIPTOR_HPP_
