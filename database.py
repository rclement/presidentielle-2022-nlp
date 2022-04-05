import io
import logging
import sqlite3
import pandas as pd
import pdfplumber
import requests

from pathlib import Path
from urllib.parse import urljoin
from bs4 import BeautifulSoup


logging.basicConfig(level=logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


db_path = (Path() / "data.db").resolve()


def extract_text_from_pdf(pdf_data: bytes) -> str:
    pdf_text = ""
    with pdfplumber.open(io.BytesIO(pdf_data)) as pdf:
        for page in pdf.pages:
            pdf_text += page.extract_text(layout=True)
    return pdf_text


def get_data() -> pd.DataFrame:
    logger.info("Getting data")

    candidates_url = "https://www.cnccep.fr/candidats.html"
    candidate_rv = requests.get(candidates_url)
    candidate_rv.raise_for_status()

    soup = BeautifulSoup(candidate_rv.text, "html.parser")
    candidates = soup.select(".candidats .mediascandidats .inner-content")

    rows = []
    for candidate in candidates:
        data = dict(
            name = candidate.select_one("h6").text,
            program_url = urljoin(candidates_url, candidate.select_one(".lien:nth-child(3) > a").get("href")),
            easy_read_url = urljoin(candidates_url, candidate.select_one(".lien:nth-child(4) > a").get("href")),
        )

        logger.info(f'Processing candidate: {data["name"]}')

        program_rv = requests.get(data["program_url"])
        data["program_text"] = extract_text_from_pdf(program_rv.content)

        easy_read_rv = requests.get(data["easy_read_url"])
        data["easy_read_text"] = extract_text_from_pdf(easy_read_rv.content)

        rows.append(data)

    return pd.DataFrame(rows)


def save_data(data: pd.DataFrame) -> None:
    logger.info(f"Saving data: {db_path}")
    db_path.unlink(missing_ok=True)

    conn = sqlite3.connect(db_path)
    data.to_sql("candidates", conn)
    conn.close()


def load_data() -> pd.DataFrame:
    logger.info(f"Loading data: {db_path}")

    conn = sqlite3.connect(Path() / "data.db")
    data = pd.read_sql("select * from candidates", conn)
    conn.close()

    return data

def main() -> None:
    data = get_data()
    save_data(data)


if __name__ == "__main__":
    main()
