package geaflow.groovy

import groovy.transform.CompileStatic
import org.apache.geaflow.api.context.RuntimeContext
import org.apache.geaflow.api.function.RichFunction
import org.apache.geaflow.api.function.io.SourceFunction
import org.apache.geaflow.api.window.IWindow

/**
 * A simple source backed by an in-memory list, modelled on the
 * FileSource class from geaflow-examples.
 */
@CompileStatic
class CollectionSource<OUT> extends RichFunction implements SourceFunction<OUT> {
    List<OUT> allRecords
    List<OUT> records
    Integer readPos = null

    CollectionSource(List<OUT> allRecords) {
        this.allRecords = allRecords
    }

    @Override
    void open(RuntimeContext runtimeContext) { }

    @Override
    void init(int parallel, int index) {
        records = parallel == 1 ? allRecords
            : (0..<allRecords.size()).findAll { it % parallel == index }.collect { allRecords[it] }
    }

    @Override
    boolean fetch(IWindow<OUT> window, SourceFunction.SourceContext<OUT> ctx) throws Exception {
        readPos ?= 0
        while (readPos < records.size()) {
            OUT out = records[readPos]
            if (window.windowId() == window.assignWindow(out)) {
                ctx.collect(out)
                readPos++
            } else {
                break
            }
        }
        readPos < records.size()
    }

    @Override
    void close() { }
}
