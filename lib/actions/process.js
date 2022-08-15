const {log, constants, helpers} = require('utils-nxg-cg');
const {producerErrorMessage} = require('msgbroker-nxg-cg');
const {default: axios} = require("axios");
const {emits} = constants;

/**
 * Method for execute query of mariadb or mysql database
 * @param msg
 * @param cfg
 * @param snapshot
 * @returns {Promise<array|object>}
 */
module.exports.process = async function process(msg, cfg, snapshot = {}) {
    try {

        log.info('Inside processQuery');
        if (JSON.stringify(msg).length <= 10000)
            log.debug('Msg=', JSON.stringify(msg));
        if (JSON.stringify(cfg).length <= 10000)
            log.debug('Config=', JSON.stringify(cfg));

        const {data} = msg;
        let message;

        if (!data) {
            throw Error(`${constants.ERROR_PROPERTY} data`);
        }

        let properties = {
            url: null,
            username: null,
            password: null
        }

        const valid = await helpers.validProperties(properties, data, cfg);

        if (valid) {
            const instance = axios.create({
                baseURL: 'https://dummyjson.com',
                headers: {'Content-Type': 'application/json'}
            });
            instance.interceptors.response.use(undefined, (e) => {
                return Promise.reject('Error user/password incorrect');
            });
            await instance.post(data.url, {
                username: data.username,
                password: data.password
            });
            message = {
                metadata: {...data},
                data: {auth: true}
            };
        }
        log.debug(message)
        this.emit(emits.data, message);
        snapshot.lastUpdated = new Date();
        this.emit(emits.snapshot, snapshot);
        log.info(constants.FINISH_EXEC);
        this.emit(emits.end);
    } catch (e) {
        log.error(e);
        this.emit(emits.error, e);
        await producerErrorMessage(msg, e);
    }

};