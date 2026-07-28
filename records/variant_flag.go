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

package records

// VariantFidelity toggles the Phase-1 VARIANT fidelity scan mode (DESIGN-variant.md
// §4.1): when true, a batch that carries a VARIANT column is emitted as whole-row
// VARIANT `V`-carrier objects (base.SigilVariant), preserving typed-scalar fidelity,
// instead of the Phase-0 JSON projection. Default false (Phase-0 read-as-JSON). A
// process-wide dev/testing knob for comparative + differential testing and
// benchmarking, intended to become the default once validated; not safe to toggle
// while a scan is running.
//
// Declared here (not parquet.go) so it's always compiled: the CLI's -variant-fidelity
// flag sets it unconditionally, and the Parquet reader that honors it is excluded from
// the `trim` build (no arrow). In a trim build the flag is simply an inert no-op.
var VariantFidelity bool
