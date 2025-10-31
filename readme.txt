
# 🐘 Mongo + Cassandra Environment Setup

## 🚀 Launching the Databases

1. Open a terminal in the project root (`~/Desktop/bd2/TPO`).

2. Start the containers:

   sudo docker-compose up -d

3. (Optional) Check that MongoDB is running:

   sudo docker exec -it mongo mongosh

4. (Optional) Check that Cassandra is running:

   sudo docker exec -it cassandra cqlsh

---

## 🧩 Setting Up the Python Environment

You’ll need Python for uploading CSV data into MongoDB and Cassandra.

1. **Install venv if not already available:**

   sudo apt install python3-venv -y

2. **Create a virtual environment:**

   python3 -m venv venv

3. **Activate it:**

 
   source venv/bin/activate


   You should now see `(venv)` at the start of your terminal prompt.

4. **Install the required libraries:**


   pip install pandas pymongo cassandra-driver


5. **Verify installation:**

   python -m pip show pymongo
   python -m pip show cassandra-driver


---

## 📦 Uploading CSV Data

Once the environment is set up and containers are running:

1. Place your CSV files in the appropriate directory (e.g. `data/`).
2. Run your Python upload script from inside the virtual environment:

   (venv) python import_data.py

*(Assuming your upload script is stored under `scripts/` — adjust as needed.)*

---

## 🧹 Stopping the Databases

When done, stop all containers:

sudo docker-compose down

---

