//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License").
//
// A minimal in-memory filesystem for the n1k1 WebAssembly demo.
//
// Go's os package under GOOS=js routes every file operation through
// globalThis.fs (see the Go runtime's syscall/fs_js.go). A browser has no real
// filesystem, so we install our own read-only fs backed by JavaScript objects:
// the sample datasets. The n1k1 engine then reads its <namespace>/<keyspace>/
// <key>.json document tree from here exactly as it would from disk.
//
// Only the read path is implemented (open/read/close/stat/fstat/lstat/readdir),
// plus write/writeSync to route Go's stdout/stderr to the console. Everything
// else returns ENOSYS -- which is why the demo runs read-only queries: writes,
// temp files and on-disk indexes all no-op, and the engine never needs them for
// in-memory-sized data.
//
// Usage:
//   installN1k1FS("/n1k1data", { default: { beers: { "21a.json": "{...}" } } });
// then load wasm_exec.js and run the module. Call BEFORE loading wasm_exec.js.

(function (globalScope) {
  "use strict";

  const S_IFDIR = 0o040000;
  const S_IFREG = 0o100000;

  function enosys() {
    const err = new Error("not implemented");
    err.code = "ENOSYS";
    return err;
  }
  function enoent(path) {
    const err = new Error("no such file or directory: " + path);
    err.code = "ENOENT";
    return err;
  }

  // Normalize a path to a canonical absolute form (collapse ".", "..", "//").
  function norm(path) {
    const parts = [];
    for (const seg of String(path).split("/")) {
      if (seg === "" || seg === ".") continue;
      if (seg === "..") parts.pop();
      else parts.push(seg);
    }
    return "/" + parts.join("/");
  }

  // Build flat dir/file maps from a nested {name: subtree|contentString} tree
  // mounted at rootPath. Directories map to a sorted child-name array; files
  // map to a Uint8Array of their bytes.
  function buildMaps(rootPath, tree) {
    const dirs = new Map(); // path -> [childName]
    const files = new Map(); // path -> Uint8Array
    const enc = new TextEncoder();

    // Ensure every ancestor of rootPath is a walkable directory ("/", "/n1k1data").
    let acc = "";
    const rootSegs = norm(rootPath).split("/").filter(Boolean);
    let prev = "/";
    if (!dirs.has("/")) dirs.set("/", []);
    for (const seg of rootSegs) {
      acc = acc + "/" + seg;
      if (!dirs.has(acc)) dirs.set(acc, []);
      const parent = dirs.get(prev);
      if (!parent.includes(seg)) parent.push(seg);
      prev = acc;
    }

    function walk(dirPath, node) {
      if (!dirs.has(dirPath)) dirs.set(dirPath, []);
      const children = dirs.get(dirPath);
      for (const name of Object.keys(node)) {
        const child = node[name];
        const childPath = norm(dirPath + "/" + name);
        if (!children.includes(name)) children.push(name);
        if (typeof child === "string") {
          files.set(childPath, enc.encode(child));
        } else if (child && typeof child === "object") {
          walk(childPath, child);
        }
      }
      children.sort();
    }
    walk(norm(rootPath), tree);
    return { dirs, files };
  }

  // Merge a second {name: subtree|content} tree, mounted at rootPath, into
  // existing dir/file maps (so a user-picked folder can sit alongside the
  // built-in demo data). Overwrites any paths that collide.
  function mergeInto(dirs, files, rootPath, tree) {
    const add = buildMaps(rootPath, tree);
    for (const [p, kids] of add.dirs) {
      if (dirs.has(p)) {
        const merged = new Set(dirs.get(p));
        for (const k of kids) merged.add(k);
        dirs.set(p, Array.from(merged).sort());
      } else {
        dirs.set(p, kids);
      }
    }
    for (const [p, bytes] of add.files) files.set(p, bytes);
  }

  globalScope.installN1k1FS = function (rootPath, tree) {
    const { dirs, files } = buildMaps(rootPath, tree);

    // Let callers mount more trees later (e.g. a folder picked via the File
    // System Access API). Returns the mounted root path.
    globalScope.n1k1MountTree = function (mountPath, mountTree) {
      mergeInto(dirs, files, mountPath, mountTree);
      return mountPath;
    };

    let nextFd = 3; // 0/1/2 reserved for stdin/stdout/stderr
    const open = new Map(); // fd -> { path }
    const decoder = new TextDecoder("utf-8");

    function statOf(path) {
      const p = norm(path);
      let mode, size;
      if (dirs.has(p)) {
        mode = S_IFDIR | 0o755;
        size = 0;
      } else if (files.has(p)) {
        mode = S_IFREG | 0o644;
        size = files.get(p).length;
      } else {
        return null;
      }
      const now = 0;
      return {
        dev: 0, ino: 0, mode: mode, nlink: 1, uid: 0, gid: 0, rdev: 0,
        size: size, blksize: 4096, blocks: Math.ceil(size / 512),
        atimeMs: now, mtimeMs: now, ctimeMs: now,
        isDirectory: () => (mode & S_IFDIR) !== 0,
        isFile: () => (mode & S_IFREG) !== 0,
      };
    }

    // stdout/stderr buffering so partial-line writes coalesce (matches the
    // default wasm_exec.js console routing).
    let outBuf = "";
    let errBuf = "";
    function flushLines(buf, sink) {
      let nl;
      while ((nl = buf.indexOf("\n")) !== -1) {
        sink(buf.substring(0, nl));
        buf = buf.substring(nl + 1);
      }
      return buf;
    }

    const fs = {
      constants: {
        O_WRONLY: 1, O_RDWR: 2, O_CREAT: 64, O_TRUNC: 512,
        O_APPEND: 1024, O_EXCL: 128, O_DIRECTORY: 65536, O_NONBLOCK: 2048,
      },

      writeSync(fd, buf) {
        const s = decoder.decode(buf);
        if (fd === 1) outBuf = flushLines(outBuf + s, (l) => console.log(l));
        else if (fd === 2) errBuf = flushLines(errBuf + s, (l) => console.error(l));
        return buf.length;
      },
      write(fd, buf, offset, length, position, callback) {
        if (offset !== 0 || length !== buf.length || position !== null) {
          callback(enosys());
          return;
        }
        const n = this.writeSync(fd, buf);
        callback(null, n);
      },

      open(path, flags, mode, callback) {
        const p = norm(path);
        if (!files.has(p) && !dirs.has(p)) {
          callback(enoent(p));
          return;
        }
        const fd = nextFd++;
        open.set(fd, { path: p });
        callback(null, fd);
      },
      close(fd, callback) {
        open.delete(fd);
        callback(null);
      },
      read(fd, buffer, offset, length, position, callback) {
        const f = open.get(fd);
        if (!f) { callback(enosys()); return; }
        const data = files.get(f.path);
        if (!data) { callback(enosys()); return; }
        const pos = position === null ? (f.pos || 0) : position;
        const n = Math.min(length, data.length - pos);
        if (n > 0) buffer.set(data.subarray(pos, pos + n), offset);
        if (position === null) f.pos = pos + Math.max(n, 0);
        callback(null, Math.max(n, 0));
      },
      fstat(fd, callback) {
        const f = open.get(fd);
        if (!f) { callback(enosys()); return; }
        callback(null, statOf(f.path));
      },
      stat(path, callback) {
        const st = statOf(path);
        if (!st) { callback(enoent(path)); return; }
        callback(null, st);
      },
      lstat(path, callback) { this.stat(path, callback); },
      readdir(path, callback) {
        const p = norm(path);
        if (!dirs.has(p)) { callback(enoent(p)); return; }
        callback(null, dirs.get(p).slice());
      },

      // Unsupported (write/mutate) operations -- the demo is read-only.
      fsync(fd, callback) { callback(null); },
      mkdir(path, perm, callback) { callback(enosys()); },
      rmdir(path, callback) { callback(enosys()); },
      unlink(path, callback) { callback(enosys()); },
      rename(from, to, callback) { callback(enosys()); },
      truncate(path, length, callback) { callback(enosys()); },
      ftruncate(fd, length, callback) { callback(enosys()); },
      chmod(path, mode, callback) { callback(enosys()); },
      fchmod(fd, mode, callback) { callback(enosys()); },
      chown(path, uid, gid, callback) { callback(enosys()); },
      fchown(fd, uid, gid, callback) { callback(enosys()); },
      lchown(path, uid, gid, callback) { callback(enosys()); },
      utimes(path, atime, mtime, callback) { callback(enosys()); },
      symlink(path, link, callback) { callback(enosys()); },
      readlink(path, callback) { callback(enosys()); },
    };

    globalScope.fs = fs;
    return fs;
  };
})(typeof globalThis !== "undefined" ? globalThis : this);
