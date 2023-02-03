FROM python:3.8-slim as builder
COPY prodRequirements.txt /requirements.txt
RUN pip install --user -r /requirements.txt

FROM python:3.8-slim as app
COPY --from=builder /root/.local /root/.local
COPY ./src ./app
WORKDIR /app/settings
RUN rm dev.ini 
RUN mv prod.ini dev.ini 
WORKDIR /app
ENV PATH=/root/.local/bin:$PATH
CMD [ "python3", "./main.py" ]