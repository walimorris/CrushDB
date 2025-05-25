# Paths to compiled class files
MICROWEBSERVER_CP="server/target/classes"
CORE_CP="core/target/classes"
UI_CP="ui/target/classes"

# (Optional) additional dependencies
DEPS="server/target/dependency/*"

# Run MicroWebServer with server, core, and ui on the classpath - currently set for development
java \
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
--add-opens java.base/sun.nio.ch=ALL-UNNAMED \
-cp "$MICROWEBSERVER_CP:$CORE_CP:$UI_CP:$DEPS" com.crushdb.server.MicroWebServer "$@"
