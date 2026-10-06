python -m venv sem4_demo1_env
source sem4_demo1_env/bin/activate
pip install -r requirements.txt
python -m ipykernel install --user --name=sem4_demo1_kernel --display-name "Python (sem4_demo1_env)"
cd ..
jupyter notebook