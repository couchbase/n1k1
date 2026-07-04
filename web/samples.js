//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License").
//
// Sample data + example queries for the n1k1 WebAssembly demo. This is plain
// data: DATASETS becomes the in-memory filesystem tree (see wasm/fs_mem.js),
// mounted so the engine sees a default/<keyspace>/<key>.json document store.
// SAMPLE_QUERIES seed the query dropdown. Edit freely -- rebuild not required
// (only index.html reads this).

// Each keyspace is { "<docKey>.json": <jsonText> }. The doc key (filename minus
// .json) is the document's META().id.
function docs(keyspace) {
  const out = {};
  for (const [id, doc] of Object.entries(keyspace)) {
    out[id + ".json"] = JSON.stringify(doc);
  }
  return out;
}

const BEERS = {
  "21a_ipa":        { name: "21A IPA",                 brewery_id: "21st_amendment", abv: 7.2,  ibu: 68, style: "American IPA",        tags: ["hoppy", "citrus"] },
  "watermelon":     { name: "Hell or High Watermelon", brewery_id: "21st_amendment", abv: 4.9,  ibu: 17, style: "Fruit Beer",         tags: ["fruit", "summer"] },
  "brew_free":      { name: "Brew Free! or Die IPA",   brewery_id: "21st_amendment", abv: 7.0,  ibu: 70, style: "American IPA",        tags: ["hoppy"] },
  "sculpin":        { name: "Sculpin IPA",             brewery_id: "ballast_point",  abv: 7.0,  ibu: 70, style: "American IPA",        tags: ["hoppy", "citrus"] },
  "victory_at_sea": { name: "Victory at Sea",          brewery_id: "ballast_point",  abv: 10.0, ibu: 60, style: "Imperial Porter",     tags: ["coffee", "vanilla"] },
  "grapefruit":     { name: "Grapefruit Sculpin",      brewery_id: "ballast_point",  abv: 7.0,  ibu: 70, style: "American IPA",        tags: ["citrus", "hoppy"] },
  "two_hearted":    { name: "Two Hearted Ale",         brewery_id: "bells",          abv: 7.0,  ibu: 55, style: "American IPA",        tags: ["hoppy", "floral"] },
  "oberon":         { name: "Oberon Ale",              brewery_id: "bells",          abv: 5.8,  ibu: 30, style: "American Pale Wheat", tags: ["wheat", "summer"] },
  "hopslam":        { name: "Hopslam Ale",             brewery_id: "bells",          abv: 10.0, ibu: 70, style: "Double IPA",          tags: ["hoppy", "honey"] },
  "guinness":       { name: "Guinness Draught",        brewery_id: "st_james_gate",  abv: 4.2,  ibu: 45, style: "Irish Dry Stout",     tags: ["roasty", "creamy"] },
};

const BREWERIES = {
  "21st_amendment": { name: "21st Amendment Brewery", city: "San Francisco", state: "CA", country: "United States", founded: 2000 },
  "ballast_point":  { name: "Ballast Point Brewing",  city: "San Diego",     state: "CA", country: "United States", founded: 1996 },
  "bells":          { name: "Bell's Brewery",         city: "Kalamazoo",     state: "MI", country: "United States", founded: 1985 },
  "st_james_gate":  { name: "St. James's Gate",       city: "Dublin",        state: "",   country: "Ireland",       founded: 1759 },
};

// The tree the fs shim mounts at /n1k1data.
const DATASETS = {
  default: {
    beers: docs(BEERS),
    breweries: docs(BREWERIES),
  },
};

const SAMPLE_QUERIES = [
  { label: "Filter + sort",
    sql: "SELECT b.name, b.abv, b.style\nFROM beers b\nWHERE b.abv >= 7\nORDER BY b.abv DESC, b.name" },
  { label: "Group + aggregate",
    sql: "SELECT b.style, COUNT(*) AS beers, ROUND(AVG(b.abv), 2) AS avg_abv, MAX(b.ibu) AS max_ibu\nFROM beers b\nGROUP BY b.style\nORDER BY beers DESC, b.style" },
  { label: "Join (ON KEYS)",
    sql: "SELECT bw.name AS brewery, bw.city, b.name AS beer, b.abv\nFROM beers b\nJOIN breweries bw ON KEYS b.brewery_id\nORDER BY brewery, beer" },
  { label: "Join + group",
    sql: "SELECT bw.country, COUNT(*) AS beers, ROUND(AVG(b.abv), 1) AS avg_abv\nFROM beers b\nJOIN breweries bw ON KEYS b.brewery_id\nGROUP BY bw.country\nORDER BY beers DESC" },
  { label: "Array (UNNEST)",
    sql: "SELECT t AS tag, COUNT(*) AS uses\nFROM beers b\nUNNEST b.tags AS t\nGROUP BY t\nORDER BY uses DESC, tag" },
  { label: "Strings + CASE",
    sql: "SELECT UPPER(b.name) AS name,\n       CASE WHEN b.abv >= 8 THEN \"strong\" WHEN b.abv >= 6 THEN \"medium\" ELSE \"sessionable\" END AS strength\nFROM beers b\nORDER BY b.abv DESC" },
  { label: "Subquery",
    sql: "SELECT bw.name, bw.city\nFROM breweries bw\nWHERE bw.founded < 2000\nORDER BY bw.founded" },
  { label: "Expressions only",
    sql: "SELECT 1 + 1 AS two,\n       ROUND(PI(), 4) AS pi,\n       UPPER(\"n1k1\") AS engine,\n       ARRAY_LENGTH([1, 2, 3]) AS len,\n       SUBSTR(\"hello world\", 0, 5) AS hi" },
  { label: "EXPLAIN",
    sql: "EXPLAIN SELECT b.style, COUNT(*)\nFROM beers b\nGROUP BY b.style" },
];
