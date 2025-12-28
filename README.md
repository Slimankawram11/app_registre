# 🗑️ Urbyn Waste Registry Transformer

A Streamlit web application to transform waste registry data from various prestataires into the Urbyn aggregated format.

## Supported Prestataires

- Veolia
- Suez
- Apeyron
- Les Alchimistes
- Paprec
- Elise
- Screlec
- Trackdechet

## 🚀 Quick Deploy to Streamlit Cloud (Recommended)

### Step 1: Create a GitHub Repository

1. Go to [github.com](https://github.com) and sign in (or create an account)
2. Click the **+** button in the top right → **New repository**
3. Name it: `waste-registry-transformer`
4. Keep it **Private** (recommended) or Public
5. Click **Create repository**

### Step 2: Upload Files

1. In your new repository, click **Add file** → **Upload files**
2. Drag and drop ALL files from this folder:
   - `app.py`
   - `etl_processor.py`
   - `requirements.txt`
   - `README.md`
3. Click **Commit changes**

### Step 3: Deploy on Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Click **New app**
4. Select:
   - **Repository**: `your-username/waste-registry-transformer`
   - **Branch**: `main`
   - **Main file path**: `app.py`
5. Click **Deploy**

### Step 4: Wait & Access

- Deployment takes 2-3 minutes
- Once ready, you'll get a URL like: `https://your-app-name.streamlit.app`
- Share this URL with your team!

---

## 💻 Run Locally (Alternative)

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

### Installation

```bash
# Clone or download the files
cd waste-registry-transformer

# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

---

## 📖 How to Use

1. **Upload Configuration Files** (sidebar):
   - ETL Mapping File (`ETL _ Mapping registre déchets prestataire vers Urbyn.xlsx`)
   - Template File (`Modèle vierge de Registre des déchets...xlsx`)

2. **Upload Data Files**:
   - Select the prestataire (or use auto-detect)
   - Upload one or more files from that prestataire

3. **Transform**:
   - Click the "Transform" button
   - Review the results and warnings

4. **Download**:
   - Click "Download Output File" to get the transformed Excel file

---

## ⚙️ Adding New Mappings

The app relies on the ETL mapping file for:
- **Waste type mappings**: Déchet sheet
- **Site mappings**: Site sheet
- **Treatment mappings**: Traitement générique sheet
- **Aggregation mappings**: Paramètres sheet

To add support for new waste types or sites, update the ETL mapping file.

---

## 🔧 Troubleshooting

### "No site mapping" warnings
- Add the missing site to the ETL file's Site sheet
- Make sure the prestataire pattern matches

### "Could not auto-detect prestataire"
- Select the prestataire manually from the dropdown
- Or rename your file to include the prestataire name

### File format errors
- Make sure Excel files are `.xlsx` format
- For CSV files, use semicolon (`;`) separator for Elise

---

## 📁 File Structure

```
waste-registry-transformer/
├── app.py              # Main Streamlit application
├── etl_processor.py    # ETL transformation logic
├── requirements.txt    # Python dependencies
└── README.md           # This file
```

---

## 🛡️ Privacy

- All processing happens in the app (no data sent elsewhere)
- Use a **Private** GitHub repository if your data is sensitive
- Streamlit Cloud processes data in memory only

---

Built for Capgemini | Urbyn Waste Registry System
