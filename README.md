# 📂 GFM — Get File Metadata

**GFM** is a lightweight tool designed to recursively scan a specified directory, extract metadata from all files within it, and export the data to a structured CSV file or MongoDB. It's ideal for file organization, auditing, reporting, or general data analysis.

---

## 🚀 Features

- 🔍 Recursively scans folders for all files  
- 📄 Extracts metadata such as:
  - File name
  - File size (in bytes)
  - File type (based on extension)
  - Creation, modification, and access timestamps
  - Absolute file path
- 📤 Saves output as a CSV file for easy viewing and processing
- 🗄️ Optionally exports metadata directly to MongoDB
- ⚙️ Simple to use and configure

---

## 📦 Installation

### 1. Clone the repository

```bash
git clone https://github.com/Mhadasavi/GFM.git
cd GFM
```
### 2. Install dependencies
Requires Python 3.6+
```bash
pip install -r requirements.txt
```
---

## 💡 Usage

#### Run the tool using:

```bash
python feeds/filemetadataexporter.py
```

You will be prompted to enter the directory to scan.

#### Output:
- A CSV file containing metadata for all scanned files.
- Optionally, metadata is exported to a MongoDB collection.

---

## 🗄️ MongoDB Integration

- The tool supports exporting file metadata to MongoDB.
- MongoDB connection details are configured in [`tools/mongo/config.py`](tools/mongo/config.py).
- Example Docker Compose setup for local MongoDB is provided in [`docker-compose.yml`](docker-compose.yml).
- Data is written to the specified collection (default: `metaDataCollection`).

#### Example MongoDB Usage

```bash
python tools/feedmasterexporter.py
```
- Reads all CSVs from a directory, merges them, and exports to both a master CSV and MongoDB.

---

## 📊 Example Output
| File Name     | Size (bytes) | Type        | Created At           | Modified At          | Path                  |
|---------------|--------------|-------------|----------------------|----------------------|-----------------------|
| document.pdf  | 12456        | PDF         | 2024-06-01 12:01:23  | 2024-06-01 12:10:12  | /home/user/docs/document.pdf |
| image.jpg     | 8456         | JPEG Image  | 2024-06-01 12:02:15  | 2024-06-01 12:02:15  | /home/user/pics/image.jpg    |

---

## 🤝 Contributing
#### Contributions are welcome!
If you'd like to add features or fix bugs:
- Fork the repository
- Create a new branch (git checkout -b feature-name)
- Commit your changes (git commit -am 'Add new feature')
- Push to the branch (git push origin feature-name)
- Open a pull request
#### Please open an issue first if you have any questions or suggestions.
--- 

## 📄 License
#### This project is licensed under the MIT License. See the LICENSE file for details.