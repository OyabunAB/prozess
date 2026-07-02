package se.oyabun.prozess

class ConsumerNotActive(message: String) : RuntimeException(message)

class PollerAlreadyRunning(message: String) : RuntimeException(message)

class PollerNotRunning(message: String) : RuntimeException(message)

class CommitFailure(message: String) : RuntimeException(message)

class CommitterAlreadyRunning(message: String) : RuntimeException(message)
