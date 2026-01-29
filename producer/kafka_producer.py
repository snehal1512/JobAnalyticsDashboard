from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime

fake = Faker("en_US")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🚀 Producing US-only job application events (state-level)...")

def infer_role(job_title):
    title = job_title.lower()
    if "engineer" in title or "developer" in title:
        return "Engineering"
    elif "data" in title or "analyst" in title:
        return "Analytics"
    elif "product" in title:
        return "Product"
    elif "marketing" in title:
        return "Marketing"
    else:
        return fake.job().split(" ")[0]

def infer_domain(job_title):
    title = job_title.lower()
    if "data" in title or "software" in title:
        return "IT"
    elif "bank" in title or "finance" in title:
        return "Finance"
    elif "health" in title or "medical" in title:
        return "Healthcare"
    else:
        return "Retail"

def infer_role_level(exp_years):
    if exp_years <= 2:
        return "Entry"
    elif exp_years <= 6:
        return "Mid"
    else:
        return "Senior"

def random_visa():
    return fake.random_element(elements=("Citizen", "H1B", "OPT", "None"))

while True:
    # Company (Faker)
    company_name = fake.company()
    company_id = abs(hash(company_name)) % 100000

    # Location (US only)
    state = fake.state()
    state_abbr = fake.state_abbr()
    city = fake.city()

    # Job
    job_title = fake.job()

    # Applicant
    experience_years = random.randint(0, 15)
    role_level = infer_role_level(experience_years)

    first_name = fake.first_name()
    last_name = fake.last_name()

    event = {
        "application": {
            "application_id": random.randint(100000, 999999),
            "application_time": datetime.utcnow().isoformat()
        },
        "job": {
            "job_id": random.randint(1, 5000),
            "job_title": job_title,
            "role": infer_role(job_title),
            "role_level": role_level,
            "domain": infer_domain(job_title),
            "industry": fake.bs().title(),
            "company_id": company_id,
            "company_name": company_name,
            "job_location": f"{city}, {state_abbr}"
        },
        "applicant": {
            "applicant_id": random.randint(1, 50000),
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}",
            "experience_years": experience_years,
            "experience_level": role_level,
            "visa_status": random_visa(),
            "region": "US",
            "state": state,
            "state_abbr": state_abbr
        }
    }

    producer.send("job_applications", event)
    print(event)
    time.sleep(1)
