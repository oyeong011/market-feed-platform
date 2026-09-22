# MDFeed C++ 데이터 평면 — 의존성 0 (컴파일러만). 상위 Makefile 의 cpp-* 타깃이 부른다.
CXX      ?= c++
CXXFLAGS ?= -std=c++20 -O2 -Wall -Wextra -Wpedantic -Werror
INC      := -Iinclude
BUILD    := build

.PHONY: all test bench clean
all: $(BUILD)/test_protocol $(BUILD)/bench_protocol $(BUILD)/conformance $(BUILD)/tcp_gateway $(BUILD)/load_client

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/test_protocol: tests/test_protocol.cpp include/mdfp/*.hpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $(INC) $< -o $@

$(BUILD)/bench_protocol: bench/bench_protocol.cpp include/mdfp/*.hpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $(INC) $< -o $@

$(BUILD)/conformance: tests/conformance.cpp include/mdfp/*.hpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $(INC) $< -o $@

$(BUILD)/tcp_gateway: src/tcp_gateway.cpp include/mdfp/*.hpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $(INC) $< -o $@

$(BUILD)/load_client: bench/load_client.cpp include/mdfp/*.hpp | $(BUILD)
	$(CXX) $(CXXFLAGS) $(INC) $< -o $@

test: $(BUILD)/test_protocol
	./$(BUILD)/test_protocol

bench: $(BUILD)/bench_protocol
	./$(BUILD)/bench_protocol $(N)

clean:
	rm -rf $(BUILD)
