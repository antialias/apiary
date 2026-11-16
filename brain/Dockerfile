# apiary-brain: the brain, shared edition. Build context: brain/.
# Stdlib-only Python on python:3.12-slim. No RUN step, so a multi-platform build needs no
# emulation. Tests and fixtures stay out of the image.
FROM python:3.12-slim
# The brain's state and credential files default to /creds; mount a volume there (or point the
# *_FILE variables elsewhere).
WORKDIR /creds
WORKDIR /app
COPY claude_usage_exporter.py notices.py brain.py classes.example.json ./
COPY providers/ ./providers/
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PORT=9115
EXPOSE 9115
CMD ["python3", "/app/brain.py"]
