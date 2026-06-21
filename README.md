# System Detekcji Anomalii Finansowych (Real-Time Fraud Detection)

Projekt zrealizowany w ramach przedmiotu Przetwarzanie Strumieni Danych i Data Science. System służy do analizy transakcji płatniczych w czasie rzeczywistym i automatycznego wykrywania nadużyć finansowych (Fraud Detection) przy użyciu Apache Flink, Apache Kafka, MongoDB oraz Streamlit.

## Architektura
System opiera się na nowoczesnej architekturze strumieniowej sterowanej zdarzeniami (Event-Driven):
- Infrastruktura (Docker): Kafka (Message Broker), Zookeeper, MongoDB (Cold Storage).
- Przetwarzanie Obliczeniowe (Docker): Klaster Apache Flink z zaawansowanym zarządzaniem stanem (Stateful Stream Processing) oraz algorytmami uczenia maszynowego (Drzewa Hoeffdinga).
- Wizualizacja (Localhost): Streamlit pełniący rolę strumieniowego monitora klasy SOC (Security Operations Center), podpięty bezpośrednio pod Kafkę.

## Wymagania systemowe
Aby uruchomić projekt, upewnij się, że masz zainstalowane:
- Docker & Docker Compose (do uruchomienia klastra, bazy i brokera)
- Python 3.10+ (zalecany 3.11.9, do uruchomienia symulatora i dashboardu)
- Java (JDK 11): Wymagane wewnętrznie przez biblioteki PyFlink.

## Struktura projektu głównych plików
- src/simulator.py: Producent danych Kafki. Generuje strumień transakcji (10 000 kart, 15 miast) z 5% szansą na celową anomalię (skoki lokalizacji, bursty, przekroczenia limitów).
- src/detector.py: Serce analityczne we Flinku. Aplikacja wysyłana na klaster Dockerowy, realizująca reguły biznesowe, okna czasowe i ML. Odkłada incydenty do Kafki i asynchronicznie do MongoDB.
- src/dashboard.py: Webowy panel analityczny na żywo, wizualizujący strukturę oszustw i przechwycone alerty prosto z tematu Kafki.
- src/test_consumer.py: Sniffer diagnostyczny (opcjonalny) do walidacji surowych danych wejściowych w Kafce na temacie "transactions".

## Instalacja i przygotowanie środowiska
Otwórz główny folder projektu w terminalu i wykonaj poniższe kroki.

1. Uruchom infrastrukturę kontenerową:
docker-compose up -d

2. Stwórz i aktywuj środowisko wirtualne Pythona:
python -m venv .venv
.\.venv\Scripts\activate (dla Windows) 
lub 
source .venv/bin/activate (dla Linux/macOS)

3. Zainstaluj wymagane biblioteki:
pip install -r requirements.txt

## Kolejność uruchomienia
Zaleca się uruchomienie procesów w osobnych oknach terminala (upewnij się, że środowisko .venv jest w nich aktywne).

TERMINAL 1: Wgranie zadania na klaster Flinka
docker-compose exec jobmanager flink run -py src/detector.py

TERMINAL 2: Uruchomienie Panelu Streamlit
streamlit run src/dashboard.py

TERMINAL 3: Uruchomienie Symulatora Transakcji
python src/simulator.py

TERMINAL 4 (Opcjonalny): Diagnostyka surowych danych
python src/test_consumer.py