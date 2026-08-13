import groovy.transform.CompileStatic

@CompileStatic
class SerCheck {
    interface Job extends Serializable {
        void run(String s)
    }

    static void doIt(String s) { }

    static void main(String[] args) {
        Job lambda = (String s) -> doIt(s)
        Job methodRef = SerCheck::doIt
        Job closure = { String s -> doIt(s) }
        [lambda: lambda, methodRef: methodRef, closure: closure].each { name, job ->
            try {
                new ObjectOutputStream(OutputStream.nullOutputStream()).writeObject(job)
                println "$name (${job.getClass().name.takeBefore('/') ?: job.getClass().name}): Java-serializable"
            } catch (e) {
                println "$name (${job.getClass().name.takeBefore('/') ?: job.getClass().name}): NOT serializable -> ${e.class.simpleName}: ${e.message?.take(90)}"
            }
        }
    }
}
