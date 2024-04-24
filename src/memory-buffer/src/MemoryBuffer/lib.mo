import ClassModule "Class";

// Return the class as default import
module {
    public let { newStableStore; new; upgrade; } = ClassModule;

    public let MemoryBuffer = ClassModule.MemoryBufferClass;
};