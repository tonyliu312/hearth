FROM python:3.12-slim

WORKDIR /app

# 不装 apt 包：原本只为 HEALTHCHECK 装 curl，改用 base 自带 Python。
# 大陆网络下 buildkit 内 apt-get(deb.debian.org) 经常 exit 100；
# pip 走清华镜像（pypi-cn-mirror 纪律，pypi.org 在大陆易 TLS 失败）。
COPY requirements.txt .
RUN pip install --no-cache-dir \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    -r requirements.txt

COPY main.py .

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=4).status==200 else 1)" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--proxy-headers"]
