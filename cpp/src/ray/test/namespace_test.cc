// Copyright 2020-2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <ray/api.h>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

int cmd_argc = 0;
char **cmd_argv = nullptr;

class SimpleActor {
 public:
  SimpleActor() {}
  static SimpleActor *FactoryCreate() { return new SimpleActor(); }
  int Ping() { return 1; }
};

RAY_REMOTE(SimpleActor::FactoryCreate, &SimpleActor::Ping);

TEST(NameSpaceTest, Test) {
  RAYLOG(INFO) << "NameSpaceTest.Test";
  ray::RayConfig config;
  config.ray_namespace = "ray_namespace1";
  ray::Init(config, cmd_argc, cmd_argv);

  ray::ActorHandle<SimpleActor> actor = ray::Actor(RAY_FUNC(SimpleActor::FactoryCreate))
                                        .SetName("simple_actor")
                                        .Remote();
  auto ref = actor.Task(&SimpleActor::Ping).Remote();
  EXPECT_EQ(1, *ref.Get());
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  cmd_argc = argc;
  cmd_argv = argv;
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();

  ray::Shutdown();
  return ret;
}
