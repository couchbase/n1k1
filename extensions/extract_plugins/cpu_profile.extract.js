// cpu_profile.extract.js — an OPAQUE recipe for cbcollect's binary Go CPU-profile logs
// (gzipped pprof protobuf: goxdcr_cprof.log, projector_cprof.log). These genuinely
// can't be framed into rows, so this recipe CLAIMS them and marks them opaque: `FROM
// `goxdcr_cprof.log`` then yields a single {kind:"opaque", note} row rather than the
// file landing in .tables' "add a *.extract.js recipe" nudge (which you can't/shouldn't
// do for a binary). The note documents what the file is and how to read it.
//
// Opaque framing reads NO file content -- it just records that the file exists and is
// intentionally unframed. See records/spec.go (FramingOpaque) and .extract help.

var match = {
  names: ["(^|/)[a-z0-9_]*_cprof\\.log$"], // goxdcr_cprof.log, projector_cprof.log, ...
  priority: 20
};

function describe(file) {
  return {
    format: "cpu_profile",
    framing: {
      kind: "opaque",
      note: "binary Go CPU profile (gzipped pprof protobuf) -- open with `go tool pprof <file>`"
    }
  };
}
