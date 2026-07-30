FROM mecha-worker-base

USER root
RUN mkdir -p /home/worker/.codex /home/worker/.local/bin /home/worker/.bun/install/global \
    && chown -R worker:worker /home/worker/.codex /home/worker/.local /home/worker/.bun \
    && chmod 777 /home/worker \
    && chmod +x /app/entrypoint-codex.sh

USER worker
ENV HOME="/home/worker"
ENV BUN_INSTALL="/home/worker/.bun"
ENV PATH="/home/worker/.local/bin:/home/worker/.bun/bin:$PATH"
ENV WORKER_BACKEND="codex"

RUN mkdir -p "$HOME/.codex-cli" && cd "$HOME/.codex-cli" \
    && bun init -y > /dev/null 2>&1 \
    && bun add --exact @openai/codex@0.145.0 \
    && ln -sf "$HOME/.codex-cli/node_modules/.bin/codex" "$HOME/.local/bin/codex" \
    && codex --version

EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s CMD curl -sf http://localhost:8080/health || exit 1
ENTRYPOINT ["/app/entrypoint-codex.sh"]
