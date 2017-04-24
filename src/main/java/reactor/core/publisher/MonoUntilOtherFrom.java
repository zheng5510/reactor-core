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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;

/**
 * Waits for a Mono source to produce a value or terminate, as well as a Publisher source
 * for which first production or termination will be used as a trigger. If the Mono source
 * produced a value, emit that value after the Mono has completed and the Publisher source
 * has either emitted once or completed. Otherwise terminate empty.
 *
 * @param <T> the value type
 *
 * @author Simon Baslé
 */
final class MonoUntilOtherFrom<T> extends Mono<T> {

	final boolean delayError;

	final Mono<T> source;

	Function<? super T, ? extends Publisher<?>>[] otherGenerators;

	MonoUntilOtherFrom(boolean delayError,
			Mono<T> monoSource,
			Function<? super T, ? extends Publisher<?>> triggerGenerator) {
		this.delayError = delayError;
		this.source = Objects.requireNonNull(monoSource, "monoSource");
		this.otherGenerators = new Function[] { Objects.requireNonNull(triggerGenerator, "triggerGenerator")};
	}

	private MonoUntilOtherFrom(boolean delayError,
			Mono<T> monoSource,
			Function<? super T, ? extends Publisher<?>>[] triggerGenerators) {
		this.delayError = delayError;
		this.source = Objects.requireNonNull(monoSource, "monoSource");
		this.otherGenerators = triggerGenerators;
	}

	/**
	 * Add a trigger generator to wait for.
	 * @param triggerGenerator
	 * @return a new {@link MonoUntilOtherFrom} instance with same source but additional trigger generator
	 */
	MonoUntilOtherFrom<T> addTriggerGenerator(Function<? super T, ? extends Publisher<?>> triggerGenerator) {
		Objects.requireNonNull(triggerGenerator, "triggerGenerator");
		Function<? super T, ? extends Publisher<?>>[] oldTriggers = this.otherGenerators;
		Function<? super T, ? extends Publisher<?>>[] newTriggers = new Function[oldTriggers.length + 1];
		System.arraycopy(oldTriggers, 0, newTriggers, 0, oldTriggers.length);
		newTriggers[oldTriggers.length] = triggerGenerator;
		return new MonoUntilOtherFrom<T>(this.delayError, this.source, newTriggers);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		UntilOtherFromCoordinator<T> parent = new UntilOtherFromCoordinator<>(s,
				delayError, otherGenerators);
		s.onSubscribe(parent);
		source.subscribe(parent);
	}

	static final class UntilOtherFromCoordinator<T>
			extends Operators.MonoSubscriber<T, T> {

		final int                                           n;
		final boolean                                       delayError;
		final Function<? super T, ? extends Publisher<?>>[] otherGenerators;

		volatile int done;
		static final AtomicIntegerFieldUpdater<UntilOtherFromCoordinator> DONE =
				AtomicIntegerFieldUpdater.newUpdater(UntilOtherFromCoordinator.class, "done");

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<UntilOtherFromCoordinator, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(UntilOtherFromCoordinator.class, Subscription.class, "s");

		UntilOtherFromTrigger[] triggerSubscribers;

		UntilOtherFromCoordinator(Subscriber<? super T> subscriber,
				boolean delayError,
				Function<? super T, ? extends Publisher<?>>[] otherGenerators) {
			super(subscriber);
			this.otherGenerators = otherGenerators;
			//don't consider the source as this is only used from when there is a value
			this.n = otherGenerators.length;

			this.delayError = delayError;
			triggerSubscribers = null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			} else {
				s.cancel();
			}
		}

		@Override
		public void onNext(T t) {
			if (value == null) {
				setValue(t);
				subscribeTriggers(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (value == null) {
				actual.onComplete();
			}
		}

		@Override
		public Object scan(Attr key) {
			switch (key) {
				case TERMINATED:
					return done;
				case PARENT:
					return actual;
				case DELAY_ERROR:
					return delayError;
			}
			return super.scan(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(triggerSubscribers);
		}

		private void subscribeTriggers(T value) {
			triggerSubscribers = new UntilOtherFromTrigger[otherGenerators.length];
			for (int i = 0; i < otherGenerators.length; i++) {
				Function<? super T, ? extends Publisher<?>> generator = otherGenerators[i];
				Publisher<?> p = generator.apply(value);
				boolean cancelOnTriggerValue = !(p instanceof Mono);
				UntilOtherFromTrigger triggerSubscriber = new UntilOtherFromTrigger(this, cancelOnTriggerValue);

				this.triggerSubscribers[i] = triggerSubscriber;
				p.subscribe(triggerSubscriber);
			}
		}

		void signalError(Throwable t) {
			if (delayError) {
				signal();
			} else {
				if (DONE.getAndSet(this, n) != n) {
					cancel();
					actual.onError(t);
				}
			}
		}

		void signal() {
			if (DONE.incrementAndGet(this) != n) {
				return;
			}

			T o = null;
			Throwable error = null;
			Throwable compositeError = null;

			//check for errors in the triggers
			for (int i = 0; i < n; i++) {
				UntilOtherFromTrigger mt = triggerSubscribers[i];
				Throwable e = mt.error;
				if (e != null) {
					if (compositeError != null) {
						compositeError.addSuppressed(e);
					} else
					if (error != null) {
						compositeError = new Throwable("Multiple errors");
						compositeError.addSuppressed(error);
						compositeError.addSuppressed(e);
					} else {
						error = e;
					}
					//else the trigger publisher was empty, but we'll ignore that
				}
			}

			if (compositeError != null) {
				actual.onError(compositeError);
			}
			else if (error != null) {
				actual.onError(error);
			} else {
				//emit the value downstream
				complete(this.value);
			}
		}

		@Override
		public void cancel() {
			if (!isCancelled()) {
				super.cancel();
				//source is always cancellable...
				Operators.terminate(S, this);
				//...but triggerSubscribers could be partially initialized
				for (int i = 0; i < triggerSubscribers.length; i++) {
					UntilOtherFromTrigger ts = triggerSubscribers[i];
					if (ts != null) ts.cancel();
				}
			}
		}
	}

	static final class UntilOtherFromTrigger<T> implements InnerConsumer<T> {

		final UntilOtherFromCoordinator<?> parent;
		final boolean                      cancelOnTriggerValue;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<UntilOtherFromTrigger, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(UntilOtherFromTrigger.class, Subscription.class, "s");

		boolean done;
		Throwable error;

		UntilOtherFromTrigger(UntilOtherFromCoordinator<?> parent,
				boolean cancelOnTriggerValue) {
			this.parent = parent;
			this.cancelOnTriggerValue = cancelOnTriggerValue;
		}

		@Override
		public Object scan(Attr key) {
			switch (key){
				case CANCELLED:
					return s == Operators.cancelledSubscription();
				case PARENT:
					return s;
				case ACTUAL:
					return parent;
				case ERROR:
					return error;
			}
			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(1);
			} else {
				s.cancel();
			}
		}

		@Override
		public void onNext(Object t) {
			if (!done) {
				done = true;
				parent.signal();
				if (cancelOnTriggerValue) {
					s.cancel();
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			parent.signalError(t);
		}

		@Override
		public void onComplete() {
			if (!done) {
				done = true;
				parent.signal();
			}
		}

		void cancel() {
			Operators.terminate(S, this);
		}
	}
}
