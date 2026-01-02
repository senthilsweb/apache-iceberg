FROM ghcr.io/projectnessie/nessie:0.93.0

EXPOSE 19120

# Use the default run script from the base image
CMD ["/opt/jboss/container/java/run/run-java.sh"]
