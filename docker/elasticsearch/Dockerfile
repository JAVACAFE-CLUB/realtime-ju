FROM docker.elastic.co/elasticsearch/elasticsearch:8.11.0

# Install Nori (Korean) analysis plugin
RUN bin/elasticsearch-plugin install --batch analysis-nori

# Verify plugin installation
RUN bin/elasticsearch-plugin list
