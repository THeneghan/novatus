# 🧪 Transaction Reconciliation & Eligibility Validation Exercise

## Problem Statement

Novatus Global operates a SaaS platform that ensures financial transactions are accurately and compliantly reported to a Global Trade Repository (GTR). Your task is to build a prototype that:

- Validates which transactions are **eligible** for reporting.
- Compares customer transactions with GTR reports to identify **mismatches**.

## 🗃️ Input Data

Sample CSVs are provided in the `data/` directory:

- `customer_transactions.csv`
- `gtr_report.csv`

## ✅ Eligibility Rules

Eligibility logic is configurable via `config.yaml`. Rules include:

- Amount > $1,000,000
- Trade type in `[New, Amend]`
- Status = `Settled`
- Region in `[EU, APAC]`

## 🔄 Reconciliation

For each eligible transaction:
- Check presence in GTR
- Compare core fields: `amount`, `currency`, `instrument_type`
- Classify mismatches

## 📤 Output

A summary report (CSV or JSON) is generated with:
- Totals
- Eligibility stats
- Reconciliation errors by type
- Sample discrepancy rows

## 🧪 Run Locally

```bash
pip install -r requirements.txt
python src/main.py
```

## 🐳 Docker

```bash
docker-compose up --build
```

## 🚀 Stretch Goals

- Convert to FastAPI service
- Integrate AWS S3 input/output
- Add Prometheus metrics
- Improve configuration modularity
