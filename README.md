# 🧠 Databricks Apps Demo  
**Build, serve, and interact with Databricks Data and AI through real applications.**

---

### 🚀 What It Is  
A working example of how to build **full-stack data apps** powered by the **Databricks Lakehouse** — combining:  
- A **Python (Dash)** front-end client  
- Shared **data modules** for Delta, permissions, and metadata  

It shows how to go from **data → API → UI → AI** all in one repo.

<p align="center">
  <a href="https://www.youtube.com/watch?v=ZbErw3mDh4E">
    <img src="https://img.youtube.com/vi/ZbErw3mDh4E/hqdefault.jpg" alt="Watch the video">
  </a>
</p>


---

### 💡 Value Proposition  
Databricks isn’t just for data pipelines — it’s an **application platform**.  
This project demonstrates how to:  
- Build **interactive apps** backed by Delta and Databricks APIs  
- Expose **secure, production-ready endpoints**  
- Enable **real-time insights and GenAI features** directly on Lakehouse data  

---

### 🧩 Structure  
```bash
DatabricksAppsDemo/
├── server/       # Rust Axum API
│   ├── data/     # Shared Rust modules (api_client, delta, metastore, permissions)
│   └── src/
├── client/       # Python Dash app
│   ├── app.py
│   └── components/
└── notebooks/    # Databricks setup / data prep
```

### Getting Started 
```
# Clone the repo
git clone https://github.com/rchynoweth/DatabricksAppsDemo.git
cd DatabricksAppsDemo

# Create Anaconda Environment
conda create -n DatabricksApps python=3.11 -y
conda activate DatabricksApps

# Deploy to Databricks
databricks apps deploy hello-world --source-code-path /Workspace/Users/rchynoweth@invisocorp.com/databricks_apps/file-uploader-app

# Keep local changes in sync 
databricks sync --watch . /Workspace/Users/rchynoweth@invisocorp.com/databricks_apps/file-uploader-app

```
