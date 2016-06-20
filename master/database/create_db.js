conn = new Mongo();
db = conn.getDB("research_db");

// Unique Indexes
// Collections for edges are each time the crawler runs, so that each crawling
// results in a new collection for a specific time
// db.edges.createIndex({ sourceId: 1, targetId: 1 }, { unique: true });
