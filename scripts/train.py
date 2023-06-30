

def make_model(input_shape):
    input_layer = keras.layers.Input(input_shape)

    conv1 = keras.layers.Conv1D(filters=128, kernel_size=3, padding="same")(input_layer)
    conv1 = keras.layers.BatchNormalization()(conv1)
    conv1 = keras.layers.ReLU()(conv1)

    conv2 = keras.layers.Conv1D(filters=128, kernel_size=3, padding="same")(conv1)
    conv2 = keras.layers.BatchNormalization()(conv2)
    conv2 = keras.layers.ReLU()(conv2)

    conv3 = keras.layers.Conv1D(filters=64, kernel_size=3, padding="same")(conv2)
    conv3 = keras.layers.BatchNormalization()(conv3)
    conv3 = keras.layers.ReLU()(conv3)

    gap = keras.layers.GlobalAveragePooling1D()(conv3)

    #output_layer1 = keras.layers.Dense(numClasses1, activation="softmax")(gap)
    output_layer2 = keras.layers.Dense(numClasses2, activation="softmax")(gap)

    return keras.models.Model(inputs=input_layer, outputs=[output_layer2])


model = make_model(input_shape=xTrain.shape[1:])
keras.utils.plot_model(model, show_shapes=True)