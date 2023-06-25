import tensorflow as tf
import matplotlib.pyplot as plt


def nnModel(inputShape, lenClasses):
    inputLayer = tf.keras.layers.Input(inputShape)

    conv1 = tf.keras.layers.Conv1D(filters=128, kernel_size=3,padding="same")(inputLayer)
    conv1 = tf.keras.layers.BatchNormalization()(conv1)
    conv1 = tf.keras.layers.ReLU()(conv1)

    conv2 = tf.keras.layers.Conv1D(filters=128, kernel_size=3, padding="same")(conv1)
    conv2 = tf.keras.layers.BatchNormalization()(conv2)
    conv2 = tf.keras.layers.ReLU()(conv2)

    conv3 = tf.keras.layers.Conv1D(filters=64, kernel_size=3, padding="same")(conv2)
    conv3 = tf.keras.layers.BatchNormalization()(conv2)
    conv3 = tf.keras.layers.ReLU()(conv3)

    gap = tf.keras.layers.GlobalAveragePooling1D()(conv3)
    
    outputLayer = tf.keras.layers.Dense(lenClasses, activation="softmax")(gap)

    return tf.keras.Model(inputs=inputLayer, outputs=outputLayer)
    
model = nnModel(inputShape=(1,5,))
tf.keras.utils.plot_model(model, show_shapes=True)

epochs = 200
batch_size = 128

callbacks = [
    tf.keras.callbacks.ModelCheckpoint(
        "best_model.h5", save_best_only=True, monitor="val_loss"
    ),
    tf.keras.callbacks.ReduceLROnPlateau(
        monitor="val_loss", factor=0.5, patience=20, min_lr=0.0001
    ),
    tf.keras.callbacks.EarlyStopping(monitor="val_loss", patience=50, verbose=1),
]
model.compile(
    optimizer="adam",
    loss="sparse_categorical_crossentropy",
    metrics=["sparse_categorical_accuracy"],
)
history = model.fit(
    xTrain,
    [yTrain],
    batch_size=batch_size,
    epochs=epochs,
    callbacks=callbacks,
    validation_split=0.2,
    verbose=1,
)

model = tf.keras.models.load_model("best_model.h5")

test_ = [model.evaluate(xTest, [yTest])]

print(test_)
#print("Test accuracy", test_acc)
#print("Test loss", test_loss)

def plotMetrics():
    metric = "sparse_categorical_accuracy"
    plt.figure()
    plt.plot(history.history[metric])
    plt.plot(history.history["val_" + metric])
    plt.title("model " + metric)
    plt.ylabel(metric, fontsize="large")
    plt.xlabel("epoch", fontsize="large")
    plt.legend(["train", "val"], loc="best")
    plt.savefig('model accuracy', bbox_inches='tight')
    plt.show()
    plt.close()