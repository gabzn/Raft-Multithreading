# Compiler to use
CXX=g++

# Compiler flags
CXXFLAGS=-pthread

# Name of the executable to build
OUTPUT=server

# Source file
SOURCE=server.cpp

# Default target
all: $(OUTPUT)  

# Rule to compile and link in one step
$(OUTPUT):   # Change $(TARGET) to $(OUTPUT) to correctly define the rule
	$(CXX) $(SOURCE) -o $(OUTPUT) $(CXXFLAGS)

# Clean up build files
clean:
	rm -f $(OUTPUT)   # Ensure the correct file is being removed
