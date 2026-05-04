# Equipment & facilities — Trace Audit

Generated: 2026-05-04  |  Source: citation_database.json

## Filter chain

| Step | Description | Count |
|------|-------------|-------|
| 0    | Full DB                                           | 2,853 |
| 1    | After noise filter (isValidEnforcementItem)        | 2,472 |
| 2    | After catFilter='Equipment & facilities'           | 86 |
| 3    | After normalisePharmaCitationKey deduplication    | 29 |

## Evidence & trust summary

| Metric | Count |
|--------|-------|
| confirmed               | 21 |
| provisional             | 7 |
| unconfirmed             | 1 |
| evidence-backed         | 28 |
| suspicious              | 1 |
| weak (mild flags only)  | 0 |

Evidence-backed = confirmed/provisional AND has ≥1 equipment/facility evidence term.

## Match reason breakdown

- **primary_gmp_category**: 29
- **failure_mode**: 1

## URL quality

- **direct_detail**: 21
- **search_landing**: 8

## Authority breakdown

- **FDA**: 29

## Top suspicious records

Records with no evidence terms OR unconfirmed classification.

### FDA | device_enforcement | unconfirmed
- Company: Steris Corporation
- Summary: Wire connected to the electrical box may shift out of its intended position, which may result in electrical arcing. Electrical arcing remains internal
- primary_gmp_category: Equipment & facilities
- Match reason: primary_gmp_category='Equipment & facilities'
- category_evidence: []
- failure_mode_evidence: []
- Flags: classification_status=unconfirmed; no equipment/facility evidence terms in category_evidence or failure_mode_evidence; category_evidence and failure_mode_evidence both empty

## First 50 displayed records

| # | Authority | Source type | Status | Evidence? | Match reason | Company |
|---|-----------|-------------|--------|-----------|--------------|---------|
| 1 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Barcelona Nut Company Inc. |
| 2 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Pure Indulgence Aesthetics |
| 3 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Henan Lvyuan Pharmaceutical Co. Ltd. |
| 4 | FDA | drug_enforcement | provisional | ✓ | primary_gmp_category='Equipment & facilities' | Xiamen Kang Zhongyuan Biotechnology Co., |
| 5 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Flowchem Pharma Private Limited |
| 6 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Vedic Lifesciences Pvt. Ltd. |
| 7 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Fareva Morton Grove |
| 8 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Flextronics America LLC |
| 9 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Winder Laboratories, LLC |
| 10 | FDA | device_enforcement | provisional | ✓ | primary_gmp_category='Equipment & facilities' | Laerdal Medical Corporation |
| 11 | FDA | device_enforcement | provisional | ✓ | primary_gmp_category='Equipment & facilities' | Medline Industries, LP |
| 12 | FDA | device_enforcement | provisional | ✓ | primary_gmp_category='Equipment & facilities' | Medline Industries, LP |
| 13 | FDA | device_enforcement | provisional | ✓ | primary_gmp_category='Equipment & facilities' | Community Products, LLC |
| 14 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Longford Water Company LLC |
| 15 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Harbor Marine Product Inc. |
| 16 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities'; failure_mode= | DeVere Manufacturing Inc. |
| 17 | FDA | device_enforcement | unconfirmed ⚠ suspicious | ✗ | primary_gmp_category='Equipment & facilities' | Steris Corporation |
| 18 | FDA | device_enforcement | provisional | ✓ | primary_gmp_category='Equipment & facilities' | Beckman Coulter, Inc. |
| 19 | FDA | device_enforcement | provisional | ✓ | primary_gmp_category='Equipment & facilities' | Datascope Corp. |
| 20 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Qianjiang Kingphar Medical Material Co L |
| 21 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | BRS Analytical Services, LLC |
| 22 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Acme United Corporation |
| 23 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | LeMaitre Vascular, Inc. |
| 24 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Mectronic Medicale S.R.L. |
| 25 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Yongdae Hwangtae Union Corp Daeryung |
| 26 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Yiling Pharmaceutical Ltd. |
| 27 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Jack & The Green Sprouts, Inc. |
| 28 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Breadbox Co. |
| 29 | FDA | warning_letter | confirmed | ✓ | primary_gmp_category='Equipment & facilities' | Mentha & Allied Products Private Ltd. |

---
_Generated by reports/equipment_facilities_trace.py_
