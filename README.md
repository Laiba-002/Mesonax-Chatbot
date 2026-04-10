# MESONEX - Manufacturing Knowledge Graph Chatbot

<div align="center">

![MESONEX](logo.jpeg)

**AI-Powered Manufacturing Intelligence System**

A sophisticated knowledge graph chatbot that transforms manufacturing data into actionable insights using Neo4j and OpenAI.

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Neo4j](https://img.shields.io/badge/Neo4j-5.15.0-green.svg)](https://neo4j.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-UI-red.svg)](https://streamlit.io/)
[![FastAPI](https://img.shields.io/badge/FastAPI-ETL-teal.svg)](https://fastapi.tiangolo.com/)

</div>

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Project Structure](#project-structure)
- [Schema](#schema)
- [Contributing](#contributing)

---

## 🎯 Overview

MESONEX is an intelligent manufacturing knowledge graph system that bridges the gap between complex manufacturing data and human understanding. It leverages:

- **Neo4j Graph Database** for hierarchical manufacturing data modeling
- **OpenAI GPT-4** for natural language query processing
- **Streamlit** for an intuitive web interface
- **FastAPI** for automated ETL operations
- **SQL Server** as the source data warehouse

The system enables users to query manufacturing data using natural language, automatically generating Cypher queries and providing intelligent insights from production hierarchies, operations, losses, and employee data.

---

## ✨ Features

### 🤖 AI-Powered Query Engine
- Natural language to Cypher query translation
- Context-aware conversation history
- Intelligent response generation with data visualization
- Support for complex manufacturing queries

### 🔄 Automated ETL Pipeline
- **Full Migration**: Complete data synchronization from SQL Server to Neo4j
- **Delta Sync**: Automatic incremental updates at configurable intervals
- Real-time data freshness tracking
- Batch processing for optimal performance

### 📊 Interactive Dashboard
- Real-time query interface
- Data visualization with Plotly charts
- Manufacturing hierarchy exploration
- Production metrics and KPIs

### 🔐 Security
- JWT-based authentication
- Plant-level access control
- Session management
- Secure credential handling

### 🏗️ Manufacturing Hierarchy Support
- **Group** → **Plant** → **Line** → **Machine**
- Production planning and operations
- Waste and downtime tracking
- Employee and role management
- Product and batch traceability

---

## 🏛️ Architecture

```
┌─────────────────┐
│   SQL Server    │ (Source Data)
└────────┬────────┘
         │
         │ ETL Pipeline (FastAPI)
         ↓
┌─────────────────┐
│     Neo4j       │ (Knowledge Graph)
└────────┬────────┘
         │
         │ Cypher Queries
         ↓
┌─────────────────┐
│  Query Engine   │ (OpenAI Integration)
└────────┬────────┘
         │
         │ Natural Language
         ↓
┌─────────────────┐
│   Streamlit UI  │ (User Interface)
└─────────────────┘
```

---

## 📦 Prerequisites

- **Python**: 3.8 or higher
- **Neo4j**: 5.15.0 or higher
- **SQL Server**: 2016 or higher with ODBC Driver 17
- **OpenAI API Key**: GPT-4 access
- **Git**: For cloning the repository

---

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/Laiba-002/Mesonax-Chatbot.git
cd Mesonax-Chatbot
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Set Up Neo4j

#### Option A: Using Docker (Recommended)

```bash
cd "Neo4j Server"
docker-compose up -d
```

#### Option B: Manual Installation

1. Download and install Neo4j from [neo4j.com/download](https://neo4j.com/download/)
2. Start the Neo4j service
3. Access Neo4j Browser at `http://localhost:7474`
4. Set password to match your configuration

### 4. Configure Environment Variables

```bash
# Copy example files
cp .env.example .env
cp config.py.example config.py
```

Edit `.env` and `config.py` with your actual credentials (see [Configuration](#configuration))

---

## ⚙️ Configuration

### Environment Variables (`.env`)

```env
# SQL Server Configuration
SQL_SERVER=your_sql_server_address
SQL_DATABASE=your_database_name
SQL_USERNAME=your_sql_username
SQL_PASSWORD=your_sql_password
SQL_DRIVER={ODBC Driver 17 for SQL Server}

# Neo4j Configuration
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_neo4j_password

# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-4o
OPENAI_TEMPERATURE=0.1
OPENAI_MAX_TOKENS=2000
```

### Configuration File (`config.py`)

The `config.py` file loads environment variables and provides default values. Key settings:

- **BATCH_SIZE**: Number of records processed per batch (default: 1000)
- **MAX_RETRY_ATTEMPTS**: Query retry limit (default: 3)
- **DELTA_SYNC_INTERVAL_SECONDS**: Auto-sync frequency (default: 3600 = 1 hour)

---

## 💻 Usage

### 1. Start the ETL API (Data Synchronization)

```bash
uvicorn etl_api:app --host 0.0.0.0 --port 8001
```

**Features:**
- Automatic full migration on startup
- Delta synchronization every hour (configurable)
- API documentation at `http://localhost:8001/docs`

**Key Endpoints:**
- `GET /status` - Check ETL status and statistics
- `POST /trigger/full` - Manual full migration
- `POST /trigger/delta` - Manual delta sync
- `GET /tables` - View migration status by table

### 2. Start the Streamlit Application

```bash
streamlit run app.py
```

Access the application at `http://localhost:8501`

**Features:**
- Natural language query interface
- Interactive data visualization
- Manufacturing hierarchy exploration
- Real-time insights

---

## 📚 API Documentation

### ETL API Endpoints

#### GET `/status`
Returns current ETL status including:
- Migration state (never_run, running, completed)
- Last full migration timestamp
- Next delta sync time
- Record counts per table

#### POST `/trigger/full`
Manually triggers a full data migration.

**Response:**
```json
{
  "message": "Full migration started",
  "timestamp": "2026-04-10T12:00:00Z"
}
```

#### POST `/trigger/delta?since=2026-04-10T10:00:00Z`
Triggers incremental sync for records modified after the specified timestamp.

**Parameters:**
- `since` (optional): ISO 8601 timestamp

#### GET `/tables?search=Production`
Lists migration statistics for all tables.

**Parameters:**
- `search` (optional): Filter tables by name

---

## 📁 Project Structure

```
Mesonax-Chatbot/
├── app.py                    # Streamlit web application
├── etl_api.py               # FastAPI ETL service
├── config.py                # Configuration management
├── config.py.example        # Configuration template
├── schema.py                # Knowledge graph schema definition
├── migrator.py              # SQL Server to Neo4j ETL logic
├── query_engine.py          # AI-powered query processor
├── jwt_auth.py              # Authentication module
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (not in git)
├── .env.example            # Environment template
├── .gitignore              # Git ignore rules
├── logo.jpeg               # Application logo
├── Neo4j Server/
│   └── docker-compose.yml  # Neo4j Docker configuration
└── README.md               # This file
```

---

## 🗂️ Schema

### Node Types

| Node | Level | Primary Key | Description |
|------|-------|-------------|-------------|
| **Group** | 1 | GroupCode | Top-level organizational unit |
| **Plant** | 2 | PlantCode | Manufacturing facility |
| **Line** | 3 | LineId | Production line |
| **Machine** | 4 | MachineCode | Manufacturing equipment |
| **Product** | 1 | ProductId | Manufactured product |
| **ProductionPlan** | 5 | OrderId | Production order |
| **POOperation** | 6 | OperationId | Production operation |
| **OperationConsumption** | 7 | ConsumptionId | Material consumption |
| **ProductionOutput** | 8 | ProductionOutputId | Production results |
| **ProductionBatch** | 9 | LotCode | Batch traceability |
| **Employees** | 1 | EmployeeCode | Workforce data |
| **Roles** | 1 | RoleId | Access roles |
| **WasteLosses** | 7 | LossId | Production waste |
| **DowntimeLosses** | 4 | LossId | Equipment downtime |
| **ProductionPlan_RelatedTasks** | 6 | TaskId | Production tasks |

### Relationship Types

- `BELONGS_TO` - Hierarchical relationships (Plant → Group, Machine → Line, etc.)
- `HAS_OPERATION` - ProductionPlan → POOperation
- `CONSUMES` - Operation → Material consumption
- `PRODUCES` - Operation → ProductionOutput
- `HAS_BATCH` - Output → ProductionBatch
- `ASSIGNED_TO` - Tasks → Employees
- `HAS_ROLE` - Employees → Roles

---

## 🔍 Example Queries

Users can ask questions in natural language:

```
"Show me all machines in Plant ABC"
"What are the top 5 products by production volume?"
"List all downtime events for Machine X in the last month"
"Which employees are assigned to production line 3?"
"Show waste losses by machine for this week"
"What is the production efficiency of Plant XYZ?"
```

---

## 🛠️ Development

### Running in Development Mode

```bash
# Start ETL API with auto-reload
uvicorn etl_api:app --reload --port 8001

# Start Streamlit with auto-reload
streamlit run app.py --server.runOnSave true
```

### Logging

Logs are written to:
- `etl_api.log` - ETL operations
- `migrator.log` - Data migration details

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📄 License

This project is proprietary software. All rights reserved.

---

## 👥 Authors

- **Development Team** - Initial work and maintenance

---

## 🙏 Acknowledgments

- **Neo4j** for the powerful graph database platform
- **OpenAI** for GPT-4 capabilities
- **Streamlit** for the intuitive UI framework
- **FastAPI** for the high-performance API framework

---

## 📞 Support

For support and queries, please contact the development team or open an issue in the repository.

---

<div align="center">

**Built with ❤️ for Manufacturing Intelligence**

</div>
