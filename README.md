# 📂 GFM — Get File Metadata

**GFM** is a lightweight tool designed to recursively scan a specified directory, extract metadata from all files within it, and export the data to a structured CSV file. It's ideal for file organization, auditing, reporting, or general data analysis.

---

## 🚀 Features

- 🔍 Recursively scans folders for all files  
- 📄 Extracts metadata such as:
  - File name
  - File size (in bytes)
  - File type (based on extension)
  - Creation and modification timestamps
- 📤 Saves output as a CSV file for easy viewing and processing
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
python gfm.py /path/to/your/folder output.csv
```
#### Arguments:

- `/path/to/your/folder:` The directory you want to scan

- `output.csv:` Path and filename for the generated CSV file
---

## 📊 Example Output
| File Name     | Size (bytes) | Type        | Created At           | Modified At          |
|---------------|--------------|-------------|-----------------------|-----------------------|
| document.pdf  | 12456        | PDF         | 2024-06-01 12:01:23   | 2024-06-01 12:10:12   |
| image.jpg     | 8456         | JPEG Image  | 2024-06-01 12:02:15   | 2024-06-01 12:02:15   |

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


