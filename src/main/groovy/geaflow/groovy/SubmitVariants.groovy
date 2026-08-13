package geaflow.groovy

import groovy.transform.CompileStatic
import org.apache.geaflow.api.graph.PGraphWindow
import org.apache.geaflow.api.pdata.stream.window.PWindowStream
import org.apache.geaflow.api.window.impl.AllWindow
import org.apache.geaflow.env.EnvironmentFactory
import org.apache.geaflow.model.graph.edge.IEdge
import org.apache.geaflow.model.graph.edge.impl.ValueEdge
import org.apache.geaflow.model.graph.vertex.IVertex
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex
import org.apache.geaflow.pipeline.PipelineFactory
import org.apache.geaflow.pipeline.task.IPipelineTaskContext
import org.apache.geaflow.pipeline.task.PipelineTask
import org.apache.geaflow.view.GraphViewBuilder
import org.apache.geaflow.view.IViewDesc

/**
 * Experiments: which Groovy ways of writing the PipelineTask survive
 * kryo serialization across the driver RPC boundary?
 *
 * Variants: lambda | methodref | closure | coerce
 */
@CompileStatic
class SubmitVariants {

    static void main(String[] args) {
        String variant = args ? args[0] : 'lambda'
        PipelineTask task
        switch (variant) {
            case 'lambda':
                task = (IPipelineTaskContext ctx) -> runJob(ctx)
                break
            case 'methodref':
                task = SubmitVariants::runJob
                break
            case 'closure':
                task = { IPipelineTaskContext ctx -> runJob(ctx) }
                break
            case 'coerce':
                task = ({ IPipelineTaskContext ctx -> runJob(ctx) } as PipelineTask)
                break
            default:
                throw new IllegalArgumentException("unknown variant: $variant")
        }
        println "=== submitting variant '$variant', task class: ${task.getClass()}"

        def environment = EnvironmentFactory.onLocalEnvironment()
        def pipeline = PipelineFactory.buildPipeline(environment)
        pipeline.submit(task)
        def result = pipeline.execute()
        result.get()
        println "=== variant '$variant' SUCCEEDED"
        environment.shutdown()
        System.exit(0)
    }

    static void runJob(IPipelineTaskContext ctx) {
        new PageRank.PRTask().execute(ctx)
    }
}
