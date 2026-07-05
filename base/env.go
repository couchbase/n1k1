//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package base

// EnvAbout maps an env name to a short description. See DefEnv().
var EnvAbout = map[string]string{}

// DefEnv meant to be friendly to `git grep base.DevEnv` to find all env vars.
func DefEnv(name, about string) string {
     EnvAbout[name] = about
     return name
}
