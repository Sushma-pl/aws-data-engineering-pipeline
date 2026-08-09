<!-- Bronze Ingestion Pipeline -->
                    ingest()
                       │
                       ▼
              File-level validation
                       │
                       ▼
              Build S3 object key
                       │
                       ▼
                Check idempotency
                  /          \
                YES           NO
                 │             │
                 ▼             ▼
             Skip upload   Schema validation
                               │
                               ▼
                        Quality validation
                           /          \
                          /            \
                    valid rows      invalid rows
                        │                │
                        │                ▼
                        │           Add error reason
                        │                │
                        │                ▼
                        │             [TODO]
                        │           Quarantine
                        │
                        ▼
                   Upload original
                      source
                        │
                        ▼
                 Write metadata to ingestion_log.parquet

<!-- validation architectur -->
Source
  │
  ▼
Validation
  │
  ├── INVALID → Reject / Quarantine
  │
  └── VALID
       │
       ▼
     Bronze
       │
       ▼
     Silver
       │
       ▼
      Gold
       │
       ▼
     Athena

    <!-- quality check -->
    HARD INVALID
├── Negative trip distance
├── Negative fare
├── Negative total amount
├── passenger_count <= 0
└── pickup > dropoff

ANOMALY / WARNING
├── passenger_count NULL
├── trip_distance = 0
├── duration = 0
└── duration > 24 hours