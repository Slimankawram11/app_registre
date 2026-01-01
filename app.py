"""
Urbyn Waste Registry Transformer
A Streamlit app to transform prestataire waste data to Urbyn format.
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime
from etl_processor import ETLProcessor, SUPPORTED_PRESTATAIRES

# Page config
st.set_page_config(
    page_title="Urbyn Waste Registry Transformer",
    page_icon="🗑️",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E88E5;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .success-box {
        padding: 1rem;
        background-color: #E8F5E9;
        border-radius: 0.5rem;
        border-left: 4px solid #4CAF50;
    }
    .warning-box {
        padding: 1rem;
        background-color: #FFF3E0;
        border-radius: 0.5rem;
        border-left: 4px solid #FF9800;
    }
    .error-box {
        padding: 1rem;
        background-color: #FFEBEE;
        border-radius: 0.5rem;
        border-left: 4px solid #F44336;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<p class="main-header">🗑️ Urbyn Waste Registry Transformer</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Transform prestataire waste data to Urbyn aggregated format</p>', unsafe_allow_html=True)

# Sidebar for configuration files
with st.sidebar:
    st.header("⚙️ Configuration Files")
    
    st.subheader("ETL Mapping File")
    etl_file = st.file_uploader(
        "Upload ETL mapping file",
        type=['xlsx'],
        key="etl_file",
        help="The ETL mapping file containing Déchet, Site, Traitement, and Paramètres sheets"
    )
    
    st.subheader("Template File")
    template_file = st.file_uploader(
        "Upload template file",
        type=['xlsx'],
        key="template_file",
        help="The Urbyn template file for output format"
    )
    
    st.divider()
    
    st.subheader("Supported Prestataires")
    for p in SUPPORTED_PRESTATAIRES:
        st.write(f"• {p}")

# Main content
col1, col2 = st.columns([2, 1])

with col1:
    st.header("Upload Data Files")
    
    # Prestataire selection
    prestataire = st.selectbox(
        "Select Prestataire",
        options=["Auto-detect"] + SUPPORTED_PRESTATAIRES,
        help="Select the prestataire or let the system auto-detect from file name"
    )
    
    # File upload
    uploaded_files = st.file_uploader(
        "Upload prestataire files",
        type=['xlsx', 'xls', 'csv'],
        accept_multiple_files=True,
        help="Upload one or more files from the selected prestataire"
    )

with col2:
    st.header("Status")
    
    # Status indicators
    if etl_file:
        st.success("✅ ETL file loaded")
    else:
        st.warning("⚠️ ETL file required")
    
    if template_file:
        st.success("✅ Template file loaded")
    else:
        st.warning("⚠️ Template file required")
    
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} file(s) uploaded")
    else:
        st.info("No data files uploaded")

# Show uploaded files
if uploaded_files:
    st.subheader("Uploaded Files")
    for f in uploaded_files:
        st.write(f"• {f.name} ({f.size / 1024:.1f} KB)")

# Transform button
st.divider()

if st.button("🔄 Transform", type="primary", use_container_width=True):
    
    # Validation
    if not etl_file:
        st.error("❌ Please upload the ETL mapping file")
    elif not template_file:
        st.error("❌ Please upload the template file")
    elif not uploaded_files:
        st.error("❌ Please upload at least one data file")
    else:
        # Process files
        with st.spinner("Processing..."):
            try:
                # Initialize processor
                processor = ETLProcessor(etl_file, template_file)
                
                # Detect prestataire if auto
                if prestataire == "Auto-detect":
                    detected = processor.detect_prestataire(uploaded_files[0].name)
                    if detected:
                        prestataire = detected
                        st.info(f"🔍 Auto-detected prestataire: **{prestataire}**")
                    else:
                        st.error("❌ Could not auto-detect prestataire. Please select manually.")
                        st.stop()
                
                # Process files
                result = processor.process(uploaded_files, prestataire)
                
                if result['success']:
                    # Success message
                    st.markdown(f"""
                    <div class="success-box">
                        <h3>✅ Transformation Complete!</h3>
                        <p><strong>{result['rows_processed']}</strong> rows transformed successfully</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Warnings
                    if result['rows_skipped'] > 0:
                        st.warning(f"⚠️ {result['rows_skipped']} rows skipped (no site mapping or incomplete data)")
                    
                    if result['warnings']:
                        with st.expander("View warnings"):
                            for w in result['warnings'][:20]:  # Show first 20
                                st.write(f"• {w}")
                            if len(result['warnings']) > 20:
                                st.write(f"... and {len(result['warnings']) - 20} more")
                    
                    # Preview
                    st.subheader("📋 Preview (first 10 rows)")
                    st.dataframe(result['data'].head(10), use_container_width=True)
                    
                    # Download button
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        result['data'].to_excel(writer, sheet_name='Registre des déchets (Mouvement', index=False)
                    output.seek(0)
                    
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"{prestataire}_Registre_Agrege_{timestamp}.xlsx"
                    
                    st.download_button(
                        label="Download Output File",
                        data=output,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                    
                else:
                    st.error(f"❌ Error: {result['error']}")
                    
            except Exception as e:
                st.error(f"❌ Error during processing: {str(e)}")
                with st.expander("View error details"):
                    import traceback
                    st.code(traceback.format_exc())

# Footer
st.divider()
st.markdown("""
<div style="text-align: center; color: #888; font-size: 0.9rem;">
    Urbyn Waste Registry Transformer | Built for Capgemini
</div>
""", unsafe_allow_html=True)
