# Inksetter

Inksetter is a proxy that sits between the OPDS server and the client running on an E-Ink device. The purpose of this app is to optimise the appearance of manga and comics as much as possible without modifying the source files. Transparently offering the same features as the original server’s OPDS feed.

Ideally, it downloads volume-sized CBZ files from a server such as [Kavita](https://www.kavitareader.com/) and delivers them to [KOReader](https://koreader.rocks/) in a format fully optimised for E-Ink displays, with particular emphasis on screens using Kaleido 3 technology like Kindle Colorsoft or Kindle Scribe Colorsoft.

This proxy supports OPDS V1, OPDS-PSE and OPDS V2. It has been extensively tested with Kavita, but the implementation is generic enough to work with other servers.

## Deployment

The recommended way to deploy this application is Docker Compose. An example `docker-compose.yml` is bundled. The application requires only one mandatory environment variable: `UPSTREAM_CATALOG` that points to the original OPDS server feed.

The application itself has a fairly simple structure, so you can deploy it in a different way on your own – in which case, however, I would advise you to ensure that the `pyvips` library uses the bundled `libvips` rather than the version installed in the system. The latter is most likely out of date and will cause issues.

## Usage

When the proxy is deployed, entering its root URL will display a list of all OPDS catalogues that can be used directly in the selected client. Every device profile has its own specific address.

Please be aware that downloading CBZ might take multiple minutes if the source CBZ is very big, high-res, or the proxy is running on a slow device. As long download not timeout, it means the process is in progress. For a number of technical reasons, the current status of the process cannot be displayed directly on the target device.

### KOReader

If KOReader is used as client to read downloaded CBZ files it **MUST BE** configured in specific way to display output correctly. Please check the [wiki](https://github.com/AcidWeb/Inksetter/wiki) for more details. Additionally, when an OPDS proxy is added to KOReader, I recommend enabling the `Use server filenames` option.

### Kavita specific features

Currently, if the proxy uses Kavita as the OPDS source, two additional features are available:

* When client download the CBZ file book cover defined at the Kavita level takes priority and will be added to file if it isn’t there already.
* If archive is missing `ComicInfo.xml` file it will be generated using Kavita metadata and injected to output.

## Current limitations

* Only CBZ files are supported.
* Manhwa/Webtoon format (long vertical strips) is currently unsuported.
* Very big CBZ (1GB+) archives might fail to download if source OPDS server don't support HTTP range requests.

## Performance

The pipeline used by this application is compute-heavy. If the level of optimisation this project is aiming for had not been as it is, this project would be a plugin for KOReader. But in its current form, it is too resource-intensive to run directly on the e-reader. This isn’t a problem with the code’s performance. It was designed that way - to offload all heavy lifting to something other than the e-reader. What’s worse, it may be too heavy for many NAS and RPi-type devices.

Performance issues manifest themselves in two ways:

* Slow page turning when OPDS-PSE is in use.
* CBZ file download timeouts.

Potential solutions to the above problems:

* Moving proxy to better hardware.
* Using smaller CBZ files as input.
* Setting environment variable `OCR_ENABLED` to `0`. This will disable part of the pipeline responsible for removing the page number from the bottom of the page and will speed up the process considerably.
* Moving to OPDS server that support HTTP range requests that allow to stream CBZ file from source and process bigger files **way more** efficiently.
