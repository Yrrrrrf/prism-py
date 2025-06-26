# examples/quickstart.py
import uvicorn
from fastapi import FastAPI
from prism.db.client import DbClient
from prism.prism import ApiPrism

# 1. Load Database Configuration from Environment
# db_url = "postgresql://a_hub_admin:password@localhost:5432/a_hub"
db_url = "postgresql://pharma_admin:password@localhost:5432/pharma_care"

# 2. Initialize FastAPI and Prism
# Create the main FastAPI app.
app = FastAPI(
    title="prism-py",
    description="A powerful REST API created directly from a database schema.",
    version="0.0.10",
)

# Initialize the Prism orchestrator.
api_prism = ApiPrism(db_client=DbClient(db_url), app=app)

# 3. Generate All API Routes
api_prism.gen_all_routes()
# api_prism.gen_metadata_routes()  # Generate /dt/* routes
# api_prism.gen_health_routes()  # Generate /health/* routes
# api_prism.gen_view_routes()  # Generate read-only routes for all views
# api_prism.gen_table_routes()  # Generate CRUD+ routes for all tables
# api_prism.gen_fn_routes()  # Generate routes for all functions
# api_prism.gen_proc_routes()  # Generate routes for all procedures
# api_prism.gen_trig_routes()  # Acknowledges triggers in the logs

api_prism.cache.log_stats()  # Log cache statistics to the console

# 4. Run the Server
if __name__ == "__main__":
    # Log connection stats and a welcome message before starting the server.
    api_prism.db_client.log_connection_stats()
    api_prism.print_welcome_message(host="127.0.0.1", port=8000)

    print(f"🚀 Starting server at http://127.0.0.1:8000")
    print("   Access API docs at http://127.0.0.1:8000/docs")
    print("   Press CTRL+C to stop.")
    uvicorn.run("__main__:app", host="127.0.0.1", port=8000, reload=True)
