/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Level;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.Fuseable;
import reactor.core.publisher.FluxBufferPredicate.Mode;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.publisher.TestPublisher;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxWindowPredicateTest extends
                                     FluxOperatorTest<String, Flux<String>> {

	@Test
	public void windowUntilNoEmptyWindows() {
		Flux.just("ALPHA", "#", "BETA", "#")
		    .windowUntil("#"::equals)
		    .flatMap(Flux::collectList)
		    .as(StepVerifier::create)
		    .assertNext(w -> assertThat(w).containsExactly("ALPHA", "#"))
		    .assertNext(w -> assertThat(w).containsExactly("BETA", "#"))
		    .verifyComplete();
	}

	@Test
	public void windowUntilCutBeforeNoEmptyWindows() {
		Flux.just("ALPHA", "#", "BETA", "#")
		    .windowUntil("#"::equals, true)
		    .flatMap(Flux::collectList)
		    .as(StepVerifier::create)
		    .assertNext(w -> assertThat(w).containsExactly("ALPHA"))
		    .assertNext(w -> assertThat(w).containsExactly("#", "BETA"))
		    .assertNext(w -> assertThat(w).containsExactly("#"))
		    .verifyComplete();
	}

	@Test
	public void windowUntilIntentionallyEmptyWindows() {
		Flux.just("ALPHA", "#", "BETA", "#", "#")
		    .windowUntil("#"::equals)
		    .flatMap(Flux::collectList)
		    .as(StepVerifier::create)
		    .assertNext(w -> assertThat(w).containsExactly("ALPHA", "#"))
		    .assertNext(w -> assertThat(w).containsExactly("BETA", "#"))
		    .assertNext(w -> assertThat(w).containsExactly("#"))
		    .verifyComplete();
	}

	@Test
	public void windowUntilCutBeforeIntentionallyEmptyWindows() {
		Flux.just("ALPHA", "#", "BETA", "#", "#")
		    .windowUntil("#"::equals, true)
		    .flatMap(Flux::collectList)
		    .as(StepVerifier::create)
		    .assertNext(w -> assertThat(w).containsExactly("ALPHA"))
		    .assertNext(w -> assertThat(w).containsExactly("#", "BETA"))
		    .assertNext(w -> assertThat(w).containsExactly("#"))
		    .assertNext(w -> assertThat(w).containsExactly("#"))
		    .verifyComplete();
	}

	@Override
	protected Scenario<String, Flux<String>> defaultScenarioOptions(Scenario<String, Flux<String>> defaultOptions) {
		return defaultOptions.shouldAssertPostTerminateState(false)
		                     .fusionMode(Fuseable.ASYNC)
		                     .fusionModeThreadBarrier(Fuseable.ANY)
		                     .prefetch(Queues.SMALL_BUFFER_SIZE);
	}

	@Override
	protected List<Scenario<String, Flux<String>>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.windowUntil(t -> true, true, 1))
						.prefetch(1)
						.receive(s -> s.buffer().subscribe(b -> Assert.fail()),
								s -> s.buffer().subscribe(b -> assertThat(b).containsExactly(item(0))),
								s -> s.buffer().subscribe(b -> assertThat(b).containsExactly(item(1))),
								s -> s.buffer().subscribe(b -> assertThat(b).containsExactly(item(2)))),

				scenario(f -> f.windowUntil(t -> true))
						.receive(s -> s.buffer().subscribe(b -> assertThat(b).containsExactly(item(0))),
								s -> s.buffer().subscribe(b -> assertThat(b).containsExactly(item(1))),
								s -> s.buffer().subscribe(b -> assertThat(b).containsExactly(item(2)))),

				scenario(f -> f.windowUntil(t -> true, false, 1))
						.prefetch(1)
						.receive(s -> s.buffer().subscribe(b -> assertThat(b).containsExactly(item(0))),
								s -> s.buffer().subscribe(b -> assertThat(b).containsExactly(item(1))),
						        s -> s.buffer().subscribe(b -> assertThat(b).containsExactly(item(2)))),

				scenario(f -> f.windowUntil(t -> false))
						.receive(s -> s.buffer().subscribe(b -> assertThat(b).containsExactly(item(0), item(1), item(2))))
		);
	}

	@Override
	protected List<Scenario<String, Flux<String>>> scenarios_operatorError() {
		return Arrays.asList(

				scenario(f -> f.windowUntil(t -> {
					throw exception();
				})),

				scenario(f -> f.windowUntil(t -> {
					throw exception();
				}, true))
		);
	}

	@Override
	protected List<Scenario<String, Flux<String>>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(f -> f.windowUntil(t -> true)),

				scenario(f -> f.windowUntil(t -> true, true))
		);
	}

	@Test
	public void apiUntil() {
		StepVerifier.create(Flux.just("red", "green", "#", "orange", "blue", "#", "black", "white")
		                        .windowUntil(color -> color.equals("#"))
		                        .flatMap(Flux::materialize)
		                        .map(s -> s.isOnComplete() ? "WINDOW CLOSED" : s.get()))
		            .expectNext("red", "green", "#", "WINDOW CLOSED")
		            .expectNext("orange", "blue", "#", "WINDOW CLOSED")
		            .expectNext("black", "white", "WINDOW CLOSED")
	                .verifyComplete();
	}

	@Test
	public void apiUntilCutAfter() {
		StepVerifier.create(Flux.just("red", "green", "#", "orange", "blue", "#", "black", "white")
		                        .windowUntil(color -> color.equals("#"), false)
		                        .flatMap(Flux::materialize)
		                        .map(s -> s.isOnComplete() ? "WINDOW CLOSED" : s.get()))
		            .expectNext("red", "green", "#", "WINDOW CLOSED")
		            .expectNext("orange", "blue", "#", "WINDOW CLOSED")
		            .expectNext("black", "white", "WINDOW CLOSED")
	                .verifyComplete();
	}

	@Test
	public void apiUntilCutBefore() {
		StepVerifier.create(Flux.just("red", "green", "#", "orange", "blue", "#", "black", "white")
		                        .windowUntil(color -> color.equals("#"), true)
		                        .flatMap(Flux::materialize)
		                        .map(s -> s.isOnComplete() ? "WINDOW CLOSED" : s.get()))
		            .expectNext("red", "green", "WINDOW CLOSED", "#")
		            .expectNext("orange", "blue", "WINDOW CLOSED", "#")
		            .expectNext("black", "white", "WINDOW CLOSED")
	                .verifyComplete();
	}

	@Test
	public void normalUntil() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntil = new FluxWindowPredicate<>(sp1,
				Queues.small(),
				Queues.unbounded(),
				Queues.SMALL_BUFFER_SIZE,
				i -> i % 3 == 0,
				Mode.UNTIL);

		StepVerifier.create(windowUntil.flatMap(Flux::materialize))
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .expectNext(Signal.next(1))
				    .then(() -> sp1.onNext(2))
		            .expectNext(Signal.next(2))
				    .then(() -> sp1.onNext(3))
		            .expectNext(Signal.next(3), Signal.complete())
				    .then(() -> sp1.onNext(4))
				    .expectNext(Signal.next(4))
				    .then(() -> sp1.onNext(5))
				    .expectNext(Signal.next(5))
				    .then(() -> sp1.onNext(6))
				    .expectNext(Signal.next(6), Signal.complete())
				    .then(() -> sp1.onNext(7))
				    .expectNext(Signal.next(7))
				    .then(() -> sp1.onNext(8))
				    .expectNext(Signal.next(8))
				    .then(sp1::onComplete)
		            .expectNext(Signal.complete())
				    .verifyComplete();

		assertThat(sp1.hasDownstreams()).isFalse();
	}

	@Test
	public void onCompletionBeforeLastBoundaryWindowEmitted() {
		Flux<Integer> source = Flux.just(1, 2);

		FluxWindowPredicate<Integer> windowUntil =
				new FluxWindowPredicate<>(source, Queues.small(), Queues.unbounded(), Queues.SMALL_BUFFER_SIZE,
						i -> i >= 3, Mode.UNTIL);

		FluxWindowPredicate<Integer> windowUntilCutBefore =
				new FluxWindowPredicate<>(source, Queues.small(), Queues.unbounded(), Queues.SMALL_BUFFER_SIZE,
						i -> i >= 3, Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(windowUntil.flatMap(Flux::collectList))
				.expectNext(Arrays.asList(1, 2))
				.expectComplete()
				.verify();

		StepVerifier.create(windowUntilCutBefore.flatMap(Flux::collectList))
		            .expectNext(Arrays.asList(1, 2))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void mainErrorUntilIsPropagatedToBothWindowAndMain() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntil = new FluxWindowPredicate<>(
				sp1, Queues.small(), Queues.unbounded(), Queues.SMALL_BUFFER_SIZE,
				i -> i % 3 == 0, Mode.UNTIL);

		StepVerifier.create(windowUntil.flatMap(Flux::materialize))
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .expectNext(Signal.next(1))
		            .then(() -> sp1.onNext(2))
		            .expectNext(Signal.next(2))
		            .then(() -> sp1.onNext(3))
		            .expectNext(Signal.next(3), Signal.complete())
		            .then(() -> sp1.onNext(4))
		            .expectNext(Signal.next(4))
		            .then(() -> sp1.onError(new RuntimeException("forced failure")))
		            //this is the error in the window:
		            .expectNextMatches(signalErrorMessage("forced failure"))
		            //this is the error in the main:
		            .expectErrorMessage("forced failure")
		            .verify();
		assertThat(sp1.hasDownstreams()).isFalse();
	}

	@Test
	public void predicateErrorUntil() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntil = new FluxWindowPredicate<>(
				sp1, Queues.small(), Queues.unbounded(), Queues.SMALL_BUFFER_SIZE,
				i -> {
					if (i == 5) throw new IllegalStateException("predicate failure");
					return i % 3 == 0;
				}, Mode.UNTIL);

		StepVerifier.create(windowUntil.flatMap(Flux::materialize))
					.expectSubscription()
					.then(() -> sp1.onNext(1))
					.expectNext(Signal.next(1))
					.then(() -> sp1.onNext(2))
					.expectNext(Signal.next(2))
					.then(() -> sp1.onNext(3))
					.expectNext(Signal.next(3), Signal.complete())
					.then(() -> sp1.onNext(4))
					.expectNext(Signal.next(4))
					.then(() -> sp1.onNext(5))
					//error in the window:
					.expectNextMatches(signalErrorMessage("predicate failure"))
					.expectErrorMessage("predicate failure")
					.verify();
		assertThat(sp1.hasDownstreams()).isFalse();
	}

	@Test
	public void normalUntilCutBefore() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntilCutBefore = new FluxWindowPredicate<>(sp1,
				Queues.small(), Queues.unbounded(), Queues.SMALL_BUFFER_SIZE,
				i -> i % 3 == 0, Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(windowUntilCutBefore.flatMap(Flux::materialize))
				.expectSubscription()
				    .then(() -> sp1.onNext(1))
				    .expectNext(Signal.next(1))
				    .then(() -> sp1.onNext(2))
				    .expectNext(Signal.next(2))
				    .then(() -> sp1.onNext(3))
				    .expectNext(Signal.complete(), Signal.next(3))
				    .then(() -> sp1.onNext(4))
				    .expectNext(Signal.next(4))
				    .then(() -> sp1.onNext(5))
				    .expectNext(Signal.next(5))
				    .then(() -> sp1.onNext(6))
				    .expectNext(Signal.complete(), Signal.next(6))
				    .then(() -> sp1.onNext(7))
				    .expectNext(Signal.next(7))
				    .then(() -> sp1.onNext(8))
				    .expectNext(Signal.next(8))
				    .then(sp1::onComplete)
				    .expectNext(Signal.complete())
				    .verifyComplete();
		assertThat(sp1.hasDownstreams()).isFalse();
	}

	@Test
	public void mainErrorUntilCutBeforeIsPropagatedToBothWindowAndMain() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntilCutBefore =
				new FluxWindowPredicate<>(sp1, Queues.small(), Queues.unbounded(), Queues.SMALL_BUFFER_SIZE,
						i -> i % 3 == 0, Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(windowUntilCutBefore.flatMap(Flux::materialize))
		            .expectSubscription()
		            .then(() -> sp1.onNext(1))
		            .expectNext(Signal.next(1))
		            .then(() -> sp1.onNext(2))
		            .expectNext(Signal.next(2))
		            .then(() -> sp1.onNext(3))
		            .expectNext(Signal.complete())
		            .expectNext(Signal.next(3))
		            .then(() -> sp1.onNext(4))
		            .expectNext(Signal.next(4))
		            .then(() -> sp1.onError(new RuntimeException("forced failure")))
		            //this is the error in the window:
		            .expectNextMatches(signalErrorMessage("forced failure"))
		            //this is the error in the main:
		            .expectErrorMessage("forced failure")
		            .verify();
		assertThat(sp1.hasDownstreams()).isFalse();
	}

	@Test
	public void predicateErrorUntilCutBefore() {
		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		FluxWindowPredicate<Integer> windowUntilCutBefore =
				new FluxWindowPredicate<>(sp1, Queues.small(), Queues.unbounded(), Queues.SMALL_BUFFER_SIZE,
				i -> {
					if (i == 5) throw new IllegalStateException("predicate failure");
					return i % 3 == 0;
				}, Mode.UNTIL_CUT_BEFORE);

		StepVerifier.create(windowUntilCutBefore.flatMap(Flux::materialize))
					.expectSubscription()
					.then(() -> sp1.onNext(1))
					.expectNext(Signal.next(1))
					.then(() -> sp1.onNext(2))
					.expectNext(Signal.next(2))
					.then(() -> sp1.onNext(3))
					.expectNext(Signal.complete(), Signal.next(3))
					.then(() -> sp1.onNext(4))
					.expectNext(Signal.next(4))
					.then(() -> sp1.onNext(5))
					//error in the window:
					.expectNextMatches(signalErrorMessage("predicate failure"))
					.expectErrorMessage("predicate failure")
					.verify();
		assertThat(sp1.hasDownstreams()).isFalse();
	}

	private <T> Predicate<? super Signal<T>> signalErrorMessage(String expectedMessage) {
		return signal -> signal.isOnError()
				&& signal.getThrowable() != null
				&& expectedMessage.equals(signal.getThrowable().getMessage());
	}

	@Test
	public void mismatchAtBeginningUntil() {
		StepVerifier.create(Flux.just("#", "red", "green")
		                        .windowUntil(s -> s.equals("#"))
		                        .flatMap(Flux::materialize)
		                        .map(sig -> sig.isOnComplete() ? "END" : sig.get()))
	                .expectNext("#", "END")
	                .expectNext("red", "green", "END")
	                .verifyComplete();
	}

	@Test
	public void mismatchAtBeginningUntilCutBefore() {
		StepVerifier.create(Flux.just("#", "red", "green")
		                        .windowUntil(s -> s.equals("#"), true)
		                        .flatMap(Flux::materialize)
		                        .map(sig -> sig.isOnComplete() ? "END" : sig.get()))
		            .expectNext("END")
	                .expectNext("#", "red", "green", "END")
	                .verifyComplete();
	}

	@Test
	public void prefetchIntegerMaxIsRequestUnboundedUntil() {
		TestPublisher<?> tp = TestPublisher.create();
		tp.flux().windowUntil(s -> true, true, Integer.MAX_VALUE).subscribe();
		tp.assertMinRequested(Long.MAX_VALUE);
	}

	@Test
	public void manualRequestWindowUntilOverRequestingSourceByPrefetch() {
		AtomicLong req = new AtomicLong();
		int prefetch = 4;

		Flux<Integer> source = Flux.range(1, 20)
		                           .doOnRequest(req::addAndGet)
		                           .log("source", Level.FINE)
		                           .hide();

		StepVerifier.create(source.windowUntil(i -> i % 5 == 0, false, prefetch)
		                          .concatMap(w -> w, 1)
				.log("downstream", Level.FINE),  0)
		            .thenRequest(2)
		            .expectNext(1, 2)
		            .thenRequest(6)
		            .expectNext(3, 4, 5, 6, 7, 8)
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenCancel()
		            .verify();

		assertThat(req.get()).isEqualTo(8 + prefetch);
	}

	@Test
	public void windowUntilUnboundedStartingDelimiterReplenishes() {
		AtomicLong req = new AtomicLong();
		Flux<String> source =
				Flux.just("#", "1A", "1B", "1C", "#", "2A", "2B", "2C", "2D", "#", "3A")
				    .hide();

		StepVerifier.create(source.doOnRequest(req::addAndGet)
		                          .log("source", Level.FINE)
		                          .windowUntil(s -> "#".equals(s), false, 2)
		                          .log("windowUntil", Level.FINE)
		                          .concatMap(w -> w.collectList()
		                                           .log("window", Level.FINE), 1)
		                          .log("downstream", Level.FINE))
		            .assertNext(l -> assertThat(l).containsExactly("#"))
		            .assertNext(l -> assertThat(l).containsExactly("1A", "1B", "1C", "#"))
		            .assertNext(l -> assertThat(l).containsExactly("2A",
				            "2B",
				            "2C",
				            "2D",
				            "#"))
		            .assertNext(l -> assertThat(l).containsExactly("3A"))
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));

		//TODO is there something wrong here? concatMap now falls back to no fusion because of THREAD_BARRIER, and this results in 15 request total, not 13
		assertThat(req.get()).isGreaterThanOrEqualTo(13); //11 elements + the prefetch
	}
}
