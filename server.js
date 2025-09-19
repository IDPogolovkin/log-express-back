require("dotenv").config();
const express = require("express");
const { Pool } = require("pg");
const cors = require("cors");
const axios = require("axios");

const app = express();

const corsOptions = {
    origin: "https://ashyq.data.gov.kz",
    origin: "http://localhost:6108",
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"]
  };

const PORT = process.env.PORT || 4000;
const SECRET_KEY = process.env.SECRET_KEY; // Secret key for authorization

// PostgreSQL connection pool
const pool = new Pool({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: process.env.PG_PORT
});

// then apply CORS (adds the headers to real requests, too)
app.use(cors(corsOptions));
// handle preflight right here, before auth
app.options("*", cors(corsOptions));

app.use(express.json());


// Middleware for authorization
function checkAuth(req, res, next) {
    // Allow preflight OPTIONS requests without auth
    if (req.method === "OPTIONS") return next();
    
    const authHeader = req.headers["authorization"];
    if (!authHeader || authHeader !== `Bearer ${SECRET_KEY}`) {
        return res.status(403).json({ error: "Unauthorized" });
    }
    next();
}

// Function to log dataset event (view/download)
async function logEvent(eventType, datasetId) {
    if (!datasetId) return { error: "Dataset ID is required" };

    const logEntry = { eventType, datasetId, timestamp: new Date().toISOString() };

    try {
        await pool.query(
            "INSERT INTO dataset_logs (event_type, dataset_id, timestamp) VALUES ($1, $2, $3)",
            [eventType, datasetId, logEntry.timestamp]
        );
        console.log(`Logged dataset ${eventType}:`, logEntry);
        return { success: true, log: logEntry };
    } catch (error) {
        console.error("Database insert error:", error);
        return { error: "Failed to insert log into database" };
    }
}

// Log dataset view
app.post("/log-dataset-view", checkAuth, async (req, res) => {
    const result = await logEvent("view", req.body.datasetId);
    res.status(result.error ? 400 : 200).json(result);
});

// Log dataset download
app.post("/log-dataset-download", checkAuth, async (req, res) => {
    const result = await logEvent("download", req.body.datasetId);
    res.status(result.error ? 400 : 200).json(result);
});

// Insert raw JSON logs into external PostgreSQL
app.post("/insert-json", checkAuth, async (req, res) => {
    const logs = req.body.logs; // Expecting an array of logs

    if (!Array.isArray(logs)) {
        return res.status(400).json({ error: "Logs must be an array" });
    }

    try {
        const values = logs.map(log => `('${log.eventType}', '${log.datasetId}', '${log.timestamp}')`).join(",");
        await pool.query(`INSERT INTO dataset_logs (event_type, dataset_id, timestamp) VALUES ${values}`);
        res.json({ success: true, message: "Logs inserted successfully" });
    } catch (error) {
        console.error("Bulk insert error:", error);
        res.status(500).json({ error: "Failed to insert logs into database" });
    }
});

app.get("/api/datasets", async (req, res) => {
    try {
        const statsResult = await pool.query(`
            WITH ranked_datasets AS (
                SELECT 
                    dataset_id,
                    COUNT(*) FILTER (WHERE event_type = 'view') AS views,
                    COUNT(*) FILTER (WHERE event_type = 'download') AS downloads,
                    -- Popularity score: 60% downloads, 40% views
                    (COUNT(*) FILTER (WHERE event_type = 'view')) * 0.4 + (COUNT(*) FILTER (WHERE event_type = 'download')) * 0.6 AS popularity_score
                FROM dataset_logs
                GROUP BY dataset_id
            )
            SELECT * FROM ranked_datasets
            ORDER BY popularity_score DESC
            LIMIT 20;
        `);

        if (statsResult.rows.length === 0) {
            return res.json([]);
        }

        const potentialDatasets = statsResult.rows;
        const enrichedDatasets = [];

        const topDatasetsStats = statsResult.rows;
        // Step 2: Iterate and fetch metadata until we have 6 valid datasets
        for (const stat of potentialDatasets) {

            if (enrichedDatasets.length >= 6) {
                break;
            }

            try {
                const magdaUrl = `https://ashyq.data.gov.kz/api/v0/registry/records/${stat.dataset_id}/aspects/dcat-dataset-strings`
                const response = await axios.get(magdaUrl);
                const metadata = response.data;

                // Function to create a short description preview
                const createPreview = (desc) => {
                    if (!desc) return "Нет описания.";
                    // Split by newlines, filter out empty lines, and join the first two.
                    const sentences = desc.split('\\n').filter(line => line.trim() !== '');
                    return sentences.slice(0, 2).join(' ') || "Нет описания.";
                };

                enrichedDatasets.push({
                    id: stat.dataset_id,
                    views: parseInt(stat.views, 10) || 0,
                    downloads: parseInt(stat.downloads, 10) || 0,
                    title: metadata.title || "Без названия",
                    publisher: metadata.publisher || "Неизвестный издатель",
                    description: createPreview(metadata.description),
                });
            } catch (error) {
                if (error.response && (error.response.status === 404 || error.response.status === 400)) {
                    console.warn(`Dataset ${stat.dataset_id} not found in Magda. Skipping.`);
                } else {
                    console.error(`An error occurred while fetching metadata for ${stat.dataset_id}:`, error.message);
                }
            }
        }

        res.json(enrichedDatasets);

    } catch (err) {
        console.error("Error fetching dataset stats:", err);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

// Start API server
app.listen(PORT, () => {
    console.log(`Dataset logging API running on port ${PORT}`);
});
