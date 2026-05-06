FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src

RUN pip install --upgrade pip \
    && pip install . \
    && useradd --system --uid 1000 chronicler \
    && mkdir -p /var/lib/chronicler \
    && chown -R chronicler:chronicler /app /var/lib/chronicler

USER chronicler

EXPOSE 8080

CMD ["chronicler-http"]
