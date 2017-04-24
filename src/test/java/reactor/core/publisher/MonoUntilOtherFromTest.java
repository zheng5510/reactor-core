/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoUntilOtherFromTest {

	@Test
	public void testMonoValuedAndPublisherVoid() {
		Publisher<Void> voidPublisher = Mono.fromRunnable(() -> { });
		
		StepVerifier.create(new MonoUntilOtherFrom<>(false, Mono.just("foo"), a -> voidPublisher))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testMonoEmptyAndPublisherVoid() {
		Publisher<Void> voidPublisher = Mono.fromRunnable(() -> { });
		StepVerifier.create(new MonoUntilOtherFrom<>(false, Mono.<String>empty(), a -> voidPublisher))
		            .verifyComplete();
	}

	@Test
	public void triggerSequenceWithDelays() {
		Duration duration = StepVerifier.create(new MonoUntilOtherFrom<>(false,
				Mono.just("foo"),
				a -> Flux.just(1, 2, 3).hide().delayElements(Duration.ofMillis(500))))
		            .expectNext("foo")
		            .verifyComplete();

		assertThat(duration.toMillis()).isGreaterThanOrEqualTo(500);
	}

	@Test
	public void triggerSequenceHasMultipleValuesCancelled() {
		AtomicBoolean triggerCancelled = new AtomicBoolean();
		StepVerifier.create(new MonoUntilOtherFrom<>(false,
				Mono.just("foo"),
				a -> Flux.just(1, 2, 3).hide()
				    .doOnCancel(() -> triggerCancelled.set(true))))
		            .expectNext("foo")
		            .verifyComplete();
		assertThat(triggerCancelled.get()).isTrue();
	}

	@Test
	public void triggerSequenceHasSingleValueNotCancelled() {
		AtomicBoolean triggerCancelled = new AtomicBoolean();
		StepVerifier.create(new MonoUntilOtherFrom<>(false,
				Mono.just("foo"),
				a -> Mono.just(1)
				    .doOnCancel(() -> triggerCancelled.set(true))))
		            .expectNext("foo")
		            .verifyComplete();
		assertThat(triggerCancelled.get()).isFalse();
	}

	@Test
	public void triggerSequenceDoneFirst() {
		StepVerifier.withVirtualTime(() -> new MonoUntilOtherFrom<>(false,
				Mono.delay(Duration.ofSeconds(2)),
				a -> Mono.just("foo")))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext(0L)
		            .verifyComplete();
	}

	@Test
	public void sourceHasError() {
		StepVerifier.create(new MonoUntilOtherFrom<>(false,
				Mono.<String>error(new IllegalStateException("boom")),
				a -> Mono.just("foo")))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void triggerHasError() {
		StepVerifier.create(new MonoUntilOtherFrom<>(false,
				Mono.just("foo"),
				a -> Mono.<String>error(new IllegalStateException("boom"))))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void sourceAndTriggerHaveErrorsNotDelayed() {
		StepVerifier.create(new MonoUntilOtherFrom<>(false,
				Mono.<String>error(new IllegalStateException("boom1")),
				a -> Mono.<Integer>error(new IllegalStateException("boom2"))))
		            .verifyErrorMessage("boom1");
	}

	@Test
	public void sourceAndTriggerHaveErrorsDelayedShortCircuits() {
		IllegalStateException boom1 = new IllegalStateException("boom1");
		IllegalStateException boom2 = new IllegalStateException("boom2");
		StepVerifier.create(new MonoUntilOtherFrom<>(true,
				Mono.<String>error(boom1),
				a -> Mono.<Integer>error(boom2)))
		            .verifyErrorMessage("boom1");
	}
	
	@Test
	public void multipleTriggersWithErrorDelayed() {
		IllegalStateException boom1 = new IllegalStateException("boom1");
		IllegalStateException boom2 = new IllegalStateException("boom2");
		StepVerifier.create(new MonoUntilOtherFrom<>(true,
				Mono.just("ok"), a -> Mono.<Integer>error(boom1))
				.untilOtherFromDelayError(a -> Mono.error(boom2))
		)
		            .verifyErrorMatches(e -> e.getMessage().equals("Multiple errors") &&
				            e.getSuppressed()[0] == boom1 &&
				            e.getSuppressed()[1] == boom2);
	}

	@Test
	public void testAPIUntilOtherFrom() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .untilOtherFrom(a -> Mono.delay(Duration.ofSeconds(2))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testAPIUntilOtherFromErrorsImmediately() {
		IllegalArgumentException boom = new IllegalArgumentException("boom");
		StepVerifier.create(Mono.error(boom)
		                        .untilOtherFrom(a -> Mono.delay(Duration.ofSeconds(2))))
		            .expectErrorMessage("boom")
		            .verify(Duration.ofMillis(200)); //at least, less than 2s
	}

	@Test
	public void testAPIUntilOtherFromDelayErrorNoError() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .untilOtherFromDelayError(a -> Mono.delay(Duration.ofSeconds(2))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testAPIUntilOtherFromDelayErrorWaitsOtherTriggers() {
		IllegalArgumentException boom = new IllegalArgumentException("boom");

		StepVerifier.withVirtualTime(() -> Mono.just("ok")
		                                       .untilOtherFromDelayError(a -> Mono.error(boom))
		                                       .untilOtherFromDelayError(a -> Mono.delay(Duration.ofSeconds(2))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void testAPIchainingCombines() {
		Mono<String> source = Mono.just("foo");

		Function<String, Flux<Integer>> generator1 = a -> Flux.just(1, 2, 3);
		Function<Object, Mono<Long>> generator2 = a -> Mono.delay(Duration.ofMillis(800));

		MonoUntilOtherFrom<String> until1 = (MonoUntilOtherFrom<String>) source.untilOtherFrom(generator1);
		MonoUntilOtherFrom<String> until2 = (MonoUntilOtherFrom<String>) until1.untilOtherFrom(generator2);

		assertThat(until1).isNotSameAs(until2);
		assertThat(until1.source).isSameAs(until2.source);
		assertThat(until1.otherGenerators).containsExactly(generator1);
		assertThat(until2.otherGenerators).containsExactly(generator1, generator2);

		StepVerifier.create(until2)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(700))
		            .thenAwait(Duration.ofMillis(100))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testAPIchainingCombinesWithFirstDelayErrorParameter() {
		Mono<String> source = Mono.just("foo");

		Function<String, Mono<String>> generator1 = a -> Mono.error(new IllegalArgumentException("boom"));
		Function<Object, Mono<Long>> generator2 = a -> Mono.delay(Duration.ofMillis(800));

		MonoUntilOtherFrom<String> until1 = (MonoUntilOtherFrom<String>) source.untilOtherFromDelayError(generator1);
		MonoUntilOtherFrom<String> until2 = (MonoUntilOtherFrom<String>) until1.untilOtherFrom(generator2);

		assertThat(until1).isNotSameAs(until2);
		assertThat(until1.source).isSameAs(until2.source);
		assertThat(until1.otherGenerators).containsExactly(generator1);
		assertThat(until2.otherGenerators).containsExactly(generator1, generator2);

		StepVerifier.create(until2)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(700))
		            .thenAwait(Duration.ofMillis(100))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void testAPIchainingUsesLongestDelayAfterValueGenerated() {
		AtomicInteger generator1Used = new AtomicInteger();
		AtomicInteger generator2Used = new AtomicInteger();

		Function<String, Mono<Long>> generator1 = a -> {
			generator1Used.incrementAndGet();
			return Mono.delay(Duration.ofMillis(400));
		};
		Function<Object, Mono<Long>> generator2 = a -> {
			generator2Used.incrementAndGet();
			return Mono.delay(Duration.ofMillis(800));
		};

		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                              .delayElement(Duration.ofSeconds(3))
		                              .untilOtherFrom(generator1)
		                              .untilOtherFrom(generator2))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(2900))
		            .then(() -> assertThat(generator1Used.get()).isZero())
		            .then(() -> assertThat(generator2Used.get()).isZero())
		            .expectNoEvent(Duration.ofMillis(100))
		            .then(() -> assertThat(generator1Used.get()).isEqualTo(1))
		            .then(() -> assertThat(generator2Used.get()).isEqualTo(1))
		            .expectNoEvent(Duration.ofMillis(700))
		            .thenAwait(Duration.ofMillis(100))
		            .expectNext("foo")
		            .then(() -> assertThat(generator1Used.get()).isEqualTo(1))
		            .then(() -> assertThat(generator2Used.get()).isEqualTo(1))
		            .verifyComplete();
	}
}