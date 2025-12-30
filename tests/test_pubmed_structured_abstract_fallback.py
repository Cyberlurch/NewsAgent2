import re

from src.newsagent2.summarizer import render_pubmed_deep_dive_from_abstract


ABSTRACT_TEXT = """Objectives: To examine how accurately ICU clinicians perceived family-reported prognostic expectations (FPEs) for patients with prolonged mechanical ventilation (PMV).

Design: A cross-sectional, exploratory design using secondary analysis.

Setting: Thirteen ICUs across five hospitals in the United States.

Subjects: Family members of patients with PMV and ICU clinicians, including physicians and nurses.

Interventions: None.

Measurements and main results: Latent profile analysis was used to identify profiles of accuracy in clinician perception of FPE, followed by bivariate analyses and multinomial logistic regression to examine associations between patient, family, and clinician characteristics and profile membership. A total of 554 participants (239 family members, 150 physicians, and 165 nurses) were included. Five distinct latent profiles of accuracy in clinician perception of FPE were identified: 1) clinician underestimation of FPE; 2) clinician overestimation of FPE; 3) accurate perception: low prognosis; 4) accurate perception: moderate prognosis; and 5) accurate perception: high prognosis. Families in profile 1 (clinician underestimation of FPE) were more likely to be spouses/partners of patients and reported higher levels of hope and optimism, whereas those in profile 2 (clinician overestimation of FPE) reported lower levels. Patient characteristics, including age, employment status, admission to medical ICU, and pulmonary-related hospital diagnosis, were statistically significantly associated with the profile membership.

Conclusions: Understanding how accurately clinicians perceive FPE is vital to improving shared decision-making and developing goal-concordant care for patients with PMV. Further research examining strategies for clinicians to accurately perceive what families believe about prognosis is needed to identify potential misalignment, initiate timely and empathetic conversations, and build toward shared decision-making and goal-concordant care."""


def test_render_pubmed_deep_dive_from_abstract_generates_structured_fields():
    output = render_pubmed_deep_dive_from_abstract(ABSTRACT_TEXT)
    lower_output = output.lower()

    assert "study type:" in lower_output
    assert "cross-sectional" in lower_output
    assert "population/setting:" in lower_output
    assert re.search(r"icu", lower_output)

    assert "Primary endpoints: Not reported" not in output
    assert "Key results: Not reported" not in output
    assert "Why this matters: Not reported" not in output
    assert "Study type: Not reported" not in output
    assert "Population/setting: Not reported" not in output
