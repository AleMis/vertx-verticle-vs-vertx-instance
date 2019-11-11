package pl.aleksandermiszkiewicz.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

@RunWith(VertxUnitRunner.class)
public class VerticleVsVertxInstanceTest {

	@Test
	public void concurrentModificationWithoutVerticles(TestContext ctx) {
		Vertx vertx = Vertx.vertx();
		Map<String, String> testMap = new LinkedHashMap<>();
		int count = 10000;

		Async async = ctx.strictAsync(count);

		vertx.eventBus().<String>consumer("adr1", h -> assertion(async, ctx, () -> testMap.put(h.body(), h.address())));
		vertx.eventBus().<String>consumer("adr2", h -> assertion(async, ctx, () -> testMap.remove(h.body())));
		vertx.eventBus().<String>consumer("adr3", h -> assertion(async, ctx, () -> testMap.entrySet().stream().count()));

		Random random = new Random(100000L);
		while (--count >= 0) {
			switch (random.nextInt(3)) {
				case 0:
					vertx.eventBus().publish("adr3", "adr3");
					break;
				case 1:
					vertx.eventBus().publish("adr2", random.nextInt(100));
					break;
				case 2:
					vertx.eventBus().publish("adr1", random.nextInt(100));
					break;
			}
		}

		async.awaitSuccess();
	}

	@Test
	public void withoutConcurrentModificationWithVerticles(TestContext ctx) {
		Vertx vertx = Vertx.vertx();
		Map<String, String> testMap = new LinkedHashMap<>();
		int count = 10000;

		Async async = ctx.strictAsync(count);
		Async async2 = ctx.strictAsync(1);
		AbstractVerticle verticle = new AbstractVerticle() {
			@Override public void start(Future<Void> startFuture) throws Exception {
				super.start(startFuture);

				vertx.eventBus().<String>consumer("adr1", h -> assertion(async, ctx, () -> testMap.put(h.body(), h.address())));
				vertx.eventBus().<String>consumer("adr2", h -> assertion(async, ctx, () -> testMap.remove(h.body())));
				vertx.eventBus().<String>consumer("adr3", h -> assertion(async, ctx, () -> testMap.entrySet().stream().count()));

			}
		};
		vertx.deployVerticle(verticle, c -> async2.complete());
		async2.awaitSuccess();

		Random random = new Random(100000L);
		while (--count >= 0) {
			switch (random.nextInt(3)) {
				case 0:
					vertx.eventBus().publish("adr3", "adr3");
					break;
				case 1:
					vertx.eventBus().publish("adr2", random.nextInt(100));
					break;
				case 2:
					vertx.eventBus().publish("adr1", random.nextInt(100));
					break;
			}
		}

		async.awaitSuccess();
	}

	private void assertion(Async async, TestContext ctx, Runnable runnable) {
		try {
			async.countDown();
			runnable.run();
		} catch (Exception e) {
			e.printStackTrace();
			ctx.fail(e.getMessage());
		}
	}
}
