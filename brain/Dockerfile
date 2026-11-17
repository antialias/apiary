# apiary-brain: the brain, shared edition. Build context: brain/.
# Python on python:3.12-slim with one dependency, cryptography (credstore.py: every tenant's
# documents and credentials are sealed with the key init.sh makes). Its wheels exist for both
# published platforms, so the multi-platform build installs, never compiles. Tests and fixtures
# stay out of the image.
FROM python:3.12-slim
RUN pip install --no-cache-dir --root-user-action=ignore cryptography==45.0.4
# The brain's data lives under /creds (SWITCH_CREDS_DIR): the registry tenants.json, the sealing
# key, classes.json, and one dir per tenant; mount a volume there.
WORKDIR /creds
WORKDIR /app
COPY claude_usage_exporter.py credstore.py tenancy.py notices.py brain.py classes.example.json ./
COPY providers/ ./providers/
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PORT=9115
EXPOSE 9115
CMD ["python3", "/app/brain.py"]
