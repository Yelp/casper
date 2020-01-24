ARG CASPER_IMAGE
FROM ${CASPER_IMAGE}

RUN curl https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh > /tmp/wait-for-it.sh
RUN chmod +x /tmp/wait-for-it.sh

CMD ["sh", "-c", "/tmp/wait-for-it.sh --timeout=90 $WAIT_ON -- /code/start.sh"]
