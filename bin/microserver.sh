# Paths to compiled class files
UI_CP="ui/target/classes"
CORE_CP="core/target/classes"

# (Optional) additional dependencies
DEPS="ui/target/dependency/*"

# Run Microserver with core and ui on the classpath - currently set for development
java -cp "$UI_CP:$CORE_CP:$DEPS" com.crushdb.ui.MicroServer "$@"