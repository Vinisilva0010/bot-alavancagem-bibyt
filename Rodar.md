1) Criar a virtualenv certa
bash
cd ~/Apex_Scalper_V1

python3 -m venv .venv
Isso vai criar a pasta .venv/ com bin/activate dentro.

Ativa:

bash
source .venv/bin/activate
(Repara no ponto: .venv, não venv.)

2) Instalar as dependências do Python
Com a venv ativa ((.venv) aparecendo no prompt), instala:

bash
pip install aiohttp streamlit pandas
Se você tiver um requirements.txt no projeto, melhor ainda:

bash
pip install -r requirements.txt
aiohttp é o que está faltando pro main_orchestrator.py.

3) Rodar o Maestro de novo
Ainda com a venv ativa:

bash
cd python_brain
python main_orchestrator.py




cd ~/Apex_Scalper_V1/rust_core
cargo run
