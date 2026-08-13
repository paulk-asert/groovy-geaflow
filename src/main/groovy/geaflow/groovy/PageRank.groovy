package geaflow.groovy

import groovy.transform.CompileStatic
import org.apache.geaflow.api.function.io.SinkFunction
import org.apache.geaflow.api.graph.PGraphWindow
import org.apache.geaflow.api.graph.compute.VertexCentricCompute
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction
import org.apache.geaflow.api.graph.function.vc.VertexCentricComputeFunction
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
 * Ranks swimmers in a hypothetical social network by influence,
 * using vertex-centric PageRank on Apache GeaFlow (incubating),
 * run in a local in-process environment.
 */
@CompileStatic
class PageRank {

    static void main(String[] args) {
        def environment = EnvironmentFactory.onLocalEnvironment()
        def pipeline = PipelineFactory.buildPipeline(environment)
        pipeline.submit(new PRTask())
        def result = pipeline.execute()
        result.get()
        environment.shutdown()
        System.exit(0)
    }

    static class PRTask implements PipelineTask {
        @Override
        void execute(IPipelineTaskContext ctx) {
            var follows = [
                'Emily Seebohm'    : ['Kaylee McKeown', 'Regan Smith'],
                'Kylie Masse'      : ['Kaylee McKeown', 'Regan Smith', 'Ingrid Wilm'],
                'Regan Smith'      : ['Kaylee McKeown', 'Katharine Berkoff'],
                'Kaylee McKeown'   : ['Regan Smith', 'Emily Seebohm'],
                'Katharine Berkoff': ['Kaylee McKeown', 'Regan Smith'],
                'Ingrid Wilm'      : ['Kaylee McKeown', 'Regan Smith', 'Kylie Masse']
            ]

            List<IVertex<String, Double>> vertices = follows.keySet().collect {
                (IVertex<String, Double>) new ValueVertex<>(it, 1d)
            }
            List<IEdge<String, Integer>> edges = follows.collectMany { follower, followed ->
                followed.collect { (IEdge<String, Integer>) new ValueEdge<>(follower, it, 1) }
            }

            PWindowStream<IVertex<String, Double>> vertexSource =
                ctx.buildSource(new CollectionSource<IVertex<String, Double>>(vertices),
                    AllWindow.<IVertex<String, Double>> getInstance())
            PWindowStream<IEdge<String, Integer>> edgeSource =
                ctx.buildSource(new CollectionSource<IEdge<String, Integer>>(edges),
                    AllWindow.<IEdge<String, Integer>> getInstance())

            def graphViewDesc = GraphViewBuilder.createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(1)
                .withBackend(IViewDesc.BackendType.Memory)
                .build()

            PGraphWindow<String, Double, Integer> graph =
                ctx.buildWindowStreamGraph(vertexSource, edgeSource, graphViewDesc)

            graph.compute(new PRAlgorithm(10, 0.85d))
                .compute(1)
                .getVertices()
                .sink(new ConsoleSink())
        }
    }

    static class ConsoleSink implements SinkFunction<IVertex<String, Double>> {
        @Override
        void write(IVertex<String, Double> v) {
            printf '%-17s has influence %.2f%n', v.id, v.value
        }
    }

    static class PRAlgorithm extends VertexCentricCompute<String, Double, Integer, Double> {
        double alpha

        PRAlgorithm(long iterations, double alpha) {
            super(iterations)
            this.alpha = alpha
        }

        @Override
        VertexCentricComputeFunction<String, Double, Integer, Double> getComputeFunction() {
            new PRComputeFunction(alpha: alpha)
        }

        @Override
        VertexCentricCombineFunction<Double> getCombineFunction() {
            null
        }
    }

    static class PRComputeFunction implements VertexCentricComputeFunction<String, Double, Integer, Double> {
        double alpha
        VertexCentricComputeFunction.VertexCentricComputeFuncContext<String, Double, Integer, Double> context

        @Override
        void init(VertexCentricComputeFunction.VertexCentricComputeFuncContext<String, Double, Integer, Double> ctx) {
            context = ctx
        }

        @Override
        void compute(String vertexId, Iterator<Double> messages) {
            def vertex = context.vertex().get()
            def outEdges = context.edges().outEdges
            if (context.iterationId == 1L) {
                if (outEdges) {
                    context.sendMessageToNeighbors(vertex.value / outEdges.size() as double)
                }
            } else {
                double sum = messages.sum(0d) as double
                double pr = sum * alpha + (1 - alpha)
                context.setNewVertexValue(pr)
                if (outEdges) {
                    context.sendMessageToNeighbors(pr / outEdges.size() as double)
                }
            }
        }

        @Override
        void finish() { }
    }
}
