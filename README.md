# 🗑️ Urbyn - Transformateur de Registre des Déchets

Application web Streamlit pour transformer les données de registre des déchets de différents prestataires vers le format agrégé Urbyn.

## Prestataires Supportés

- Veolia
- Suez
- Apeyron
- Les Alchimistes
- Paprec
- Elise
- Screlec
- Trackdechet

## 🚀 Déploiement Rapide sur Streamlit Cloud (Recommandé)

### Étape 1 : Créer un Compte GitHub

1. Allez sur [github.com](https://github.com) et connectez-vous (ou créez un compte)
2. Cliquez sur le bouton **+** en haut à droite → **New repository**
3. Nom : `waste-registry-transformer`
4. Gardez-le **Private** (recommandé) ou Public
5. Cliquez sur **Create repository**

### Étape 2 : Téléverser les Fichiers

1. Dans votre nouveau dépôt, cliquez sur **Add file** → **Upload files**
2. Glissez-déposez TOUS les fichiers de ce dossier :
   - `app.py`
   - `etl_processor.py`
   - `requirements.txt`
   - `README.md`
3. Cliquez sur **Commit changes**

### Étape 3 : Déployer sur Streamlit Cloud

1. Allez sur [share.streamlit.io](https://share.streamlit.io)
2. Connectez-vous avec votre compte GitHub
3. Cliquez sur **New app**
4. Sélectionnez :
   - **Repository** : `votre-nom/waste-registry-transformer`
   - **Branch** : `main`
   - **Main file path** : `app.py`
5. Cliquez sur **Deploy**

### Étape 4 : Attendre et Accéder

- Le déploiement prend 2-3 minutes
- Une fois prêt, vous obtiendrez une URL comme : `https://votre-app.streamlit.app`
- Partagez cette URL avec votre équipe !

---

## 💻 Exécuter Localement (Alternative)

### Prérequis

- Python 3.8 ou supérieur
- pip (gestionnaire de paquets Python)

### Installation

```bash
# Clonez ou téléchargez les fichiers
cd waste-registry-transformer

# Installez les dépendances
pip install -r requirements.txt

# Lancez l'application
streamlit run app.py
```

L'application s'ouvrira dans votre navigateur à `http://localhost:8501`

---

## 📖 Comment Utiliser

1. **Téléverser les Fichiers de Configuration** (barre latérale) :
   - Fichier ETL Mapping (`ETL _ Mapping registre déchets prestataire vers Urbyn.xlsx`)
   - Fichier Modèle (`Modèle vierge de Registre des déchets...xlsx`)

2. **Téléverser les Fichiers de Données** :
   - Sélectionnez le prestataire (ou utilisez la détection automatique)
   - Téléversez un ou plusieurs fichiers de ce prestataire

3. **Transformer** :
   - Cliquez sur le bouton "Transform"
   - Vérifiez les résultats et les avertissements

4. **Télécharger** :
   - Cliquez sur "Download Output File" pour obtenir le fichier Excel transformé

---

## ⚙️ Ajouter de Nouveaux Mappings

L'application utilise le fichier ETL mapping pour :
- **Mappings des types de déchets** : Feuille Déchet
- **Mappings des sites** : Feuille Site
- **Mappings des traitements** : Feuille Traitement générique
- **Mappings d'agrégation** : Feuille Paramètres

Pour ajouter le support de nouveaux types de déchets ou sites, mettez à jour le fichier ETL mapping.

---

## 🔧 Dépannage

### Avertissements "No site mapping"
- Ajoutez le site manquant dans la feuille Site du fichier ETL
- Vérifiez que le pattern du prestataire correspond

### "Could not auto-detect prestataire"
- Sélectionnez le prestataire manuellement dans le menu déroulant
- Ou renommez votre fichier pour inclure le nom du prestataire

### Erreurs de format de fichier
- Assurez-vous que les fichiers Excel sont au format `.xlsx`
- Pour les fichiers CSV, utilisez le séparateur point-virgule (`;`) pour Elise

---

## 📁 Structure des Fichiers

```
waste-registry-transformer/
├── app.py              # Application Streamlit principale
├── etl_processor.py    # Logique de transformation ETL
├── requirements.txt    # Dépendances Python
└── README.md           # Ce fichier
```

---

## 🛡️ Confidentialité

- Tout le traitement se fait dans l'application (aucune donnée envoyée ailleurs)
- Utilisez un dépôt GitHub **Privé** si vos données sont sensibles
- Streamlit Cloud traite les données en mémoire uniquement

---

Développé pour Capgemini | Système Urbyn de Registre des Déchets
